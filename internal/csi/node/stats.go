package node

import (
	"context"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"math"
)

func validGrowth(required int64) bool { return required > 0 && required <= math.MaxInt64-((1<<30)-1) }

func validExpansionRange(required, limit int64) bool {
	return required >= 0 && limit >= 0 && (required == 0 || validGrowth(required)) && (limit == 0 || required <= limit)
}

func (r *FilesystemRuntime) ownedPath(ctx context.Context, s LocalState, path string) (Device, error) {
	if s.Node != r.Node() {
		return Device{}, protocol.Fail(protocol.Blocked, "local volume binding differs")
	}
	if s.StagePath != path {
		if _, ok := s.Targets[path]; !ok {
			return Device{}, protocol.Fail(protocol.NotFound, "path not owned by this volume")
		}
	}
	snap, e := r.Inspector.Snapshot(ctx)
	if e != nil {
		return Device{}, e
	}
	d, e := selected(snap, s.Volume.Serial)
	if e != nil {
		return d, e
	}
	m, ok, e := mounted(snap, path)
	if e != nil {
		return d, e
	}
	if !ok {
		return d, protocol.Fail(protocol.NotFound, "volume path not mounted")
	}
	if m.MajorMinor != d.MajorMinor {
		return d, protocol.Fail(protocol.Blocked, "volume path source differs")
	}
	return d, nil
}
func (r *FilesystemRuntime) Stats(ctx context.Context, handle, path string) (usage Usage, err error) {
	if path == "" {
		return usage, protocol.Fail(protocol.Invalid, "volume path required")
	}
	err = r.Journal.WithLock(ctx, handle, func() error {
		s, e := r.Journal.Load(handle)
		if e != nil {
			return e
		}
		// A non-owned volume/path is NOT_FOUND even if the supplied path is
		// not canonical. Do not inspect or dereference that caller path.
		if s.StagePath != path {
			if _, owned := s.Targets[path]; !owned {
				return protocol.Fail(protocol.NotFound, "path not owned by this volume")
			}
		}
		if e = r.Paths.Check(path, "any", false); e != nil {
			return e
		}
		if _, e = r.ownedPath(ctx, s, path); e != nil {
			return e
		}
		usage, e = r.Filesystem.Stats(ctx, path)
		if e != nil {
			return e
		}
		if usage.TotalBytes < 0 || usage.UsedBytes < 0 || usage.AvailableBytes < 0 || usage.UsedBytes > usage.TotalBytes || usage.AvailableBytes > usage.TotalBytes || usage.TotalInodes < 0 || usage.UsedInodes < 0 || usage.AvailableInodes < 0 || usage.UsedInodes > usage.TotalInodes || usage.AvailableInodes > usage.TotalInodes {
			return protocol.Fail(protocol.RecoveryRequired, "invalid actual filesystem usage")
		}
		return nil
	})
	return
}
func (r *FilesystemRuntime) Expand(ctx context.Context, p protocol.StagePermit, path string, required, limit int64) (size int64, err error) {
	if !validExpansionRange(required, limit) {
		return 0, protocol.Fail(protocol.Invalid, "invalid expansion range")
	}
	if err = r.Paths.Check(path, "any", false); err != nil {
		return
	}
	err = r.Journal.WithLock(ctx, p.Volume.Handle, func() error {
		s, e := r.load(ctx, p)
		if e != nil {
			return e
		}
		if p.Initialization != "FormatComplete" || s.Initialization != "FormatComplete" {
			return protocol.Fail(protocol.Blocked, "initialized volume required for expansion")
		}
		d, e := r.ownedPath(ctx, s, path)
		if e != nil {
			return e
		}
		sigs, e := r.Filesystem.Probe(ctx, d)
		if e != nil {
			return e
		}
		if !ownSignature(sigs, p.Volume.FSUUID) {
			return protocol.Fail(protocol.Blocked, "expand filesystem signature differs")
		}
		d.FSType = "ext4"
		d.FSUUID = p.Volume.FSUUID
		size, e = r.Filesystem.Expand(ctx, d, required, limit)
		if e != nil {
			return e
		}
		if size < required {
			return protocol.Fail(protocol.RecoveryRequired, "actual expansion size below request")
		}
		if limit > 0 && size > limit {
			return protocol.Fail(protocol.RecoveryRequired, "actual expansion size exceeds limit")
		}
		return nil
	})
	return
}
