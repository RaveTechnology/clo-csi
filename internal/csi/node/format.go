package node

import (
	"context"
	"errors"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/google/uuid"
	"time"
)

type Signature struct{ Type, UUID string }
type Usage struct{ TotalBytes, UsedBytes, AvailableBytes, TotalInodes, UsedInodes, AvailableInodes int64 }
type Filesystem interface {
	Probe(context.Context, Device) ([]Signature, error)
	Format(context.Context, Device, string) error
	Check(context.Context, Device) error
	Mount(context.Context, Device, string) error
	Bind(context.Context, string, string, bool) error
	Remount(context.Context, string, string, bool) error
	Unmount(context.Context, string) error
	VerifyUnmounted(context.Context, Device) error
	Stats(context.Context, string) (Usage, error)
	Expand(context.Context, Device, int64, int64) (int64, error)
}
type StageResult struct {
	NeedsReport       bool
	Snapshot          protocol.Snapshot
	FilesystemChecked bool
}
type VolumeRuntime interface {
	Snapshot(context.Context) (protocol.Snapshot, error)
	Stage(context.Context, protocol.StagePermit, string) (StageResult, error)
	Publish(context.Context, protocol.StagePermit, string, string, bool) error
	Unpublish(context.Context, string, string) error
	Unstage(context.Context, string, string) error
	Stats(context.Context, string, string) (Usage, error)
	Expand(context.Context, protocol.StagePermit, string, int64, int64) (int64, error)
	Local(string) (LocalState, error)
}
type FilesystemRuntime struct {
	Inspector  Inspector
	Journal    *Journal
	Filesystem Filesystem
	Paths      Paths
	Node       func() protocol.NodeRef
}

var ErrNeedFormat = errors.New("initial format authorization required")

func (r *FilesystemRuntime) Snapshot(ctx context.Context) (protocol.Snapshot, error) {
	return r.Inspector.Snapshot(ctx)
}
func (r *FilesystemRuntime) Local(handle string) (LocalState, error) { return r.Journal.Load(handle) }
func selected(snapshot protocol.Snapshot, serial string) (Device, error) {
	d, e := SelectDevice(snapshot, serial)
	if e != nil {
		return d, e
	}
	for _, other := range snapshot.Disks {
		if other.Root && other.MajorMinor == d.MajorMinor {
			return d, protocol.Fail(protocol.Blocked, "device aliases root block identity")
		}
	}
	return d, nil
}
func ownSignature(signatures []Signature, uuid string) bool {
	if len(signatures) == 0 {
		return false
	}
	for _, s := range signatures {
		if s.Type != "ext4" || s.UUID != uuid {
			return false
		}
	}
	return true
}
func enhance(snapshot protocol.Snapshot, serial, fsUUID string) protocol.Snapshot {
	for i := range snapshot.Disks {
		if snapshot.Disks[i].Serial == serial {
			snapshot.Disks[i].FSType = "ext4"
			snapshot.Disks[i].FSUUID = fsUUID
		}
	}
	return snapshot
}
func (r *FilesystemRuntime) load(ctx context.Context, p protocol.StagePermit) (LocalState, error) {
	if p.AllowFormat && p.Initialization == "FormatComplete" {
		return LocalState{}, protocol.Fail(protocol.Blocked, "completed initialization conflicts with new format permission")
	}
	if p.Node != r.Node() || p.Volume.Handle == "" || p.Volume.RecordID == "" || p.Volume.Serial == "" || p.Volume.AttachEpoch < 1 || p.Volume.FSUUID != p.Volume.ProviderID {
		return LocalState{}, protocol.Fail(protocol.Blocked, "local volume/node permit identity differs")
	}
	if _, e := uuid.Parse(p.Volume.ProviderID); e != nil {
		return LocalState{}, protocol.Fail(protocol.Invalid, "invalid provider/FS UUID")
	}
	state, e := r.Journal.Load(p.Volume.Handle)
	if missing(e) {
		return LocalState{Key: p.Volume.Handle, Volume: p.Volume, Node: p.Node, Targets: map[string]bool{}, PendingTargets: map[string]bool{}}, nil
	}
	if e != nil {
		return state, e
	}
	if state.Volume.ProviderID != p.Volume.ProviderID || state.Volume.Serial != p.Volume.Serial || state.Volume.FSUUID != p.Volume.FSUUID || state.Volume.RecordID != p.Volume.RecordID || state.Volume.AttachEpoch > p.Volume.AttachEpoch {
		return state, protocol.Fail(protocol.Blocked, "local immutable volume or stale attachment differs")
	}
	if state.Volume.AttachEpoch < p.Volume.AttachEpoch {
		snap, e := r.Inspector.Snapshot(ctx)
		if e != nil {
			return state, e
		}
		d, e := selected(snap, p.Volume.Serial)
		if e != nil {
			return state, e
		}
		if state.StagePath != "" || len(state.Targets) > 0 || len(state.PendingTargets) > 0 || len(d.Mounts) > 0 {
			return state, protocol.Fail(protocol.Blocked, "old attachment still has mount references")
		}
		state.Volume = p.Volume
		state.Node = p.Node
		state.Barrier = false
		state.DetachCommitted = false
		state.DetachReceipt = nil
	} else if state.Node != p.Node {
		return state, protocol.Fail(protocol.Blocked, "local binding differs")
	}
	if state.Barrier {
		if state.DetachCommitted || !p.ResumeQuiesced || p.Initialization != "FormatComplete" {
			return state, protocol.Fail(protocol.Blocked, "local detach barrier is closed")
		}
		state.Barrier = false
		state.DetachReceipt = nil
	}
	if state.Targets == nil {
		state.Targets = map[string]bool{}
	}
	if state.PendingTargets == nil {
		state.PendingTargets = map[string]bool{}
	}
	return state, nil
}
func mounted(snapshot protocol.Snapshot, target string) (protocol.Mount, bool, error) {
	var mount protocol.Mount
	found := false
	for _, d := range snapshot.Disks {
		for _, m := range d.Mounts {
			if m.Target != target {
				continue
			}
			if found {
				return mount, false, protocol.Fail(protocol.Exists, "multiple mounts cover target")
			}
			mount = m
			found = true
		}
	}
	return mount, found, nil
}
func checkMount(snapshot protocol.Snapshot, d Device, target string, ro bool) (bool, error) {
	m, ok, e := mounted(snapshot, target)
	if e != nil {
		return false, e
	}
	if !ok {
		return false, nil
	}
	if m.MajorMinor != d.MajorMinor || m.ReadOnly != ro {
		return true, protocol.Fail(protocol.Exists, "existing mount has a different source or read-only mode")
	}
	return true, nil
}
func (r *FilesystemRuntime) Stage(ctx context.Context, p protocol.StagePermit, stage string) (result StageResult, err error) {
	if err = r.Paths.Check(stage, "stage", true); err != nil {
		return result, err
	}
	err = r.Journal.WithLock(ctx, p.Volume.Handle, func() error {
		state, e := r.load(ctx, p)
		if e != nil {
			return e
		}
		snap, e := r.Inspector.Snapshot(ctx)
		if e != nil {
			return e
		}
		d, e := selected(snap, p.Volume.Serial)
		if e != nil {
			return e
		}
		sigs, e := r.Filesystem.Probe(ctx, d)
		if e != nil {
			return e
		}
		owned := ownSignature(sigs, p.Volume.FSUUID)
		blank := len(sigs) == 0 && d.FSType == "" && d.FSUUID == ""
		if !owned && !blank {
			return protocol.Fail(protocol.RecoveryRequired, "foreign or incomplete filesystem signature")
		}
		if blank {
			if state.Initialization != "" || (!p.AllowFormat && p.Initialization != "") {
				return protocol.Fail(protocol.RecoveryRequired, "previous format state cannot be formatted again")
			}
			if !p.AllowFormat {
				return ErrNeedFormat
			}
			if p.OperationID == "" || p.Nonce == "" || len(d.Mounts) > 0 {
				return protocol.Fail(protocol.Blocked, "format intent or unmounted device proof missing")
			}
			state.Initialization = "FormatDispatched"
			state.FormatOperationID = p.OperationID
			state.FormatNonce = p.Nonce
			if e = r.Journal.Save(state); e != nil {
				return e
			}
			if e = r.Filesystem.Format(ctx, d, p.Volume.FSUUID); e != nil {
				return protocol.Fail(protocol.RecoveryRequired, "format was dispatched; inspect filesystem, no retry mkfs")
			}
			snap, e = r.Inspector.Snapshot(ctx)
			if e != nil {
				return e
			}
			d, e = selected(snap, p.Volume.Serial)
			if e != nil {
				return e
			}
			sigs, e = r.Filesystem.Probe(ctx, d)
			if e != nil {
				return e
			}
			if !ownSignature(sigs, p.Volume.FSUUID) {
				return protocol.Fail(protocol.RecoveryRequired, "format did not create the recorded filesystem")
			}
			owned = true
		}
		if owned && p.Initialization != "FormatComplete" {
			if !p.AllowFormat && p.Initialization != "FormatDispatched" {
				return protocol.Fail(protocol.RecoveryRequired, "registry has no filesystem initialization intent")
			}
			if len(d.Mounts) > 0 {
				return protocol.Fail(protocol.RecoveryRequired, "uncertain format check requires unmounted device")
			}
			d.FSType = "ext4"
			d.FSUUID = p.Volume.FSUUID
			if e = r.Filesystem.Check(ctx, d); e != nil {
				return protocol.Fail(protocol.RecoveryRequired, "read-only filesystem check failed")
			}
			state.Initialization = "FormatComplete"
			state.FormatOperationID = p.OperationID
			state.FormatNonce = p.Nonce
			if e = r.Journal.Save(state); e != nil {
				return e
			}
			result = StageResult{NeedsReport: true, FilesystemChecked: true, Snapshot: enhance(snap, d.Serial, p.Volume.FSUUID)}
			return nil
		}
		if p.Initialization != "FormatComplete" {
			return protocol.Fail(protocol.Blocked, "filesystem initialization is not complete")
		}
		d.FSType = "ext4"
		d.FSUUID = p.Volume.FSUUID
		if state.StagePath != "" && state.StagePath != stage {
			return protocol.Fail(protocol.Exists, "volume staged at another target")
		}
		exists, e := checkMount(snap, d, stage, false)
		if e != nil {
			return e
		}
		state.Initialization = "FormatComplete"
		state.StagePath = stage
		if e = r.Journal.Save(state); e != nil {
			return e
		}
		if !exists {
			if len(d.Mounts) > 0 {
				return protocol.Fail(protocol.Blocked, "device mounted outside recorded staging path")
			}
			if e = r.Filesystem.Mount(ctx, d, stage); e != nil {
				return e
			}
		}
		actual, e := r.Inspector.Snapshot(ctx)
		if e != nil {
			return e
		}
		exists, e = checkMount(actual, d, stage, false)
		if e != nil {
			return e
		}
		if !exists {
			return protocol.Fail(protocol.Unavailable, "staging mount not observed")
		}
		result.Snapshot = enhance(actual, d.Serial, p.Volume.FSUUID)
		return r.Journal.Save(state)
	})
	return
}
func (r *FilesystemRuntime) Unstage(ctx context.Context, handle, stage string) error {
	if e := r.Paths.Check(stage, "stage", false); e != nil {
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
			return protocol.Fail(protocol.Blocked, "local unstage binding differs")
		}
		if len(s.Targets) > 0 || len(s.PendingTargets) > 0 {
			return protocol.Fail(protocol.Blocked, "pod mount references remain")
		}
		if s.StagePath != "" && s.StagePath != stage {
			return protocol.Fail(protocol.Exists, "stage target differs")
		}
		if s.StagePath == "" {
			snapshot, e := r.Inspector.Snapshot(ctx)
			if e != nil {
				return e
			}
			if _, mounted, e := mounted(snapshot, stage); e != nil {
				return e
			} else if mounted {
				return protocol.Fail(protocol.Blocked, "unrecorded mount at unstaged target")
			}
			return nil
		}
		snap, e := r.Inspector.Snapshot(ctx)
		if e != nil {
			return e
		}
		d, e := selected(snap, s.Volume.Serial)
		if e != nil {
			return e
		}
		m, ok, e := mounted(snap, stage)
		if e != nil {
			return e
		}
		if ok && (s.StagePath != stage || m.MajorMinor != d.MajorMinor) {
			return protocol.Fail(protocol.Blocked, "stage mount is not owned")
		}
		s.Barrier = true
		if e = r.Journal.Save(s); e != nil {
			return e
		}
		if ok {
			if e = r.Filesystem.Unmount(ctx, stage); e != nil {
				return e
			}
		}
		snap, e = r.Inspector.Snapshot(ctx)
		if e != nil {
			return e
		}
		d, e = selected(snap, s.Volume.Serial)
		if e != nil {
			return e
		}
		if len(d.Mounts) > 0 {
			return protocol.Fail(protocol.Blocked, "device still has host mount references")
		}
		if e = r.Paths.RemoveEmpty(stage, "stage"); e != nil {
			return e
		}
		s.StagePath = ""
		s.DetachReceipt = &protocol.Receipt{Type: "unstaged", Node: s.Node, Volume: s.Volume, Snapshot: snap, Quiesced: true, ObservedAt: time.Now().UTC()}
		return r.Journal.Save(s)
	})
}
