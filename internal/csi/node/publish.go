package node

import (
	"context"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
)

func (r *FilesystemRuntime) Publish(ctx context.Context, p protocol.StagePermit, stage, target string, readonly bool) error {
	if e := r.Paths.Check(stage, "stage", false); e != nil {
		return e
	}
	if e := r.Paths.Check(target, "target", true); e != nil {
		return e
	}
	return r.Journal.WithLock(ctx, p.Volume.Handle, func() error {
		s, e := r.load(ctx, p)
		if e != nil {
			return e
		}
		if p.Initialization != "FormatComplete" || s.Initialization != "FormatComplete" || s.StagePath != stage {
			return protocol.Fail(protocol.Blocked, "owned initialized staging mount required")
		}
		snap, e := r.Inspector.Snapshot(ctx)
		if e != nil {
			return e
		}
		d, e := selected(snap, p.Volume.Serial)
		if e != nil {
			return e
		}
		exists, e := checkMount(snap, d, stage, false)
		if e != nil {
			return e
		}
		if !exists {
			return protocol.Fail(protocol.Blocked, "staging mount absent")
		}
		m, exists, e := mounted(snap, target)
		if e != nil {
			return e
		}
		if exists {
			if m.MajorMinor != d.MajorMinor {
				return protocol.Fail(protocol.Exists, "target source differs")
			}
			if recorded, ok := s.Targets[target]; ok {
				if recorded != readonly || m.ReadOnly != readonly {
					return protocol.Fail(protocol.Exists, "existing target read-only state differs")
				}
				return nil
			}
			pending, written := s.PendingTargets[target]
			if !written || pending != readonly {
				return protocol.Fail(protocol.Exists, "existing target has no matching publish intent")
			}
			if m.ReadOnly != readonly {
				if e = r.Filesystem.Remount(ctx, stage, target, readonly); e != nil {
					return e
				}
			}
		} else {
			if recorded, ok := s.Targets[target]; ok && recorded != readonly {
				return protocol.Fail(protocol.Exists, "target request differs from saved ownership")
			}
			s.PendingTargets[target] = readonly
			if e = r.Journal.Save(s); e != nil {
				return e
			}
			if e = r.Filesystem.Bind(ctx, stage, target, readonly); e != nil {
				return e
			}
		}
		actual, e := r.Inspector.Snapshot(ctx)
		if e != nil {
			return e
		}
		exists, e = checkMount(actual, d, target, readonly)
		if e != nil {
			return e
		}
		if !exists {
			return protocol.Fail(protocol.Unavailable, "bind mount not observed")
		}
		s.Targets[target] = readonly
		delete(s.PendingTargets, target)
		return r.Journal.Save(s)
	})
}
func (r *FilesystemRuntime) Unpublish(ctx context.Context, handle, target string) error {
	if e := r.Paths.Check(target, "target", false); e != nil {
		return e
	}
	return r.Journal.WithLock(ctx, handle, func() error {
		s, e := r.Journal.Load(handle)
		if missing(e) {
			return nil
		}
		if e != nil {
			return e
		}
		if s.Node != r.Node() {
			return protocol.Fail(protocol.Blocked, "local unpublish binding differs")
		}
		_, owned := s.Targets[target]
		_, pending := s.PendingTargets[target]
		if !owned && !pending {
			return nil
		}
		snap, e := r.Inspector.Snapshot(ctx)
		if e != nil {
			return e
		}
		m, exists, e := mounted(snap, target)
		if e != nil {
			return e
		}
		if exists {
			d, e := selected(snap, s.Volume.Serial)
			if e != nil {
				return e
			}
			if m.MajorMinor != d.MajorMinor {
				return protocol.Fail(protocol.Blocked, "unpublish target belongs to another source")
			}
			if e = r.Filesystem.Unmount(ctx, target); e != nil {
				return e
			}
			actual, e := r.Inspector.Snapshot(ctx)
			if e != nil {
				return e
			}
			if _, still, e := mounted(actual, target); e != nil {
				return e
			} else if still {
				return protocol.Fail(protocol.Unavailable, "target unmount not confirmed")
			}
		}
		if e = r.Paths.RemoveEmpty(target, "target"); e != nil {
			return e
		}
		delete(s.Targets, target)
		delete(s.PendingTargets, target)
		return r.Journal.Save(s)
	})
}
