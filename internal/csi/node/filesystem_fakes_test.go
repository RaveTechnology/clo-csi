package node

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"path/filepath"
	"sync"
	"testing"
)

const fsVolumeUUID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
const fsHandle = "clo:11111111-1111-4111-8111-111111111111:" + fsVolumeUUID

type fsInspector struct {
	mu sync.Mutex
	s  protocol.Snapshot
}

func (i *fsInspector) Snapshot(context.Context) (protocol.Snapshot, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	b, _ := json.Marshal(i.s)
	var out protocol.Snapshot
	_ = json.Unmarshal(b, &out)
	return out, nil
}
func (i *fsInspector) BySerial(ctx context.Context, serial string) (Device, error) {
	s, e := i.Snapshot(ctx)
	if e != nil {
		return Device{}, e
	}
	return SelectDevice(s, serial)
}

type fakeFilesystem struct {
	ins                                               *fsInspector
	j                                                 *Journal
	formats, mounts, binds, unmounts, expands, checks int
	bindCrash                                         bool
	lastDevice, lastUUID                              string
	formatFailure                                     string
	checkErr, expandErr                               error
	usage                                             Usage
}

func (f *fakeFilesystem) Probe(_ context.Context, d Device) ([]Signature, error) {
	f.ins.mu.Lock()
	defer f.ins.mu.Unlock()
	for _, disk := range f.ins.s.Disks {
		if disk.Serial == d.Serial && disk.FSType != "" {
			return []Signature{{Type: disk.FSType, UUID: disk.FSUUID}}, nil
		}
	}
	return nil, nil
}
func (f *fakeFilesystem) Format(_ context.Context, d Device, uuid string) error {
	state, e := f.j.Load(fsHandle)
	if e != nil || state.Initialization != "FormatDispatched" {
		return errors.New("format intent was not durable")
	}
	f.formats++
	f.lastDevice = d.Path
	f.lastUUID = uuid
	f.ins.mu.Lock()
	for idx := range f.ins.s.Disks {
		if f.ins.s.Disks[idx].Serial == d.Serial {
			f.ins.s.Disks[idx].FSType = "ext4"
			f.ins.s.Disks[idx].FSUUID = uuid
			if f.formatFailure == "partial" {
				f.ins.s.Disks[idx].FSUUID = "partial-foreign"
			}
		}
	}
	f.ins.mu.Unlock()
	if f.formatFailure != "" {
		return errors.New("injected format crash")
	}
	return nil
}
func (f *fakeFilesystem) Check(context.Context, Device) error           { f.checks++; return f.checkErr }
func (f *fakeFilesystem) VerifyUnmounted(context.Context, Device) error { return nil }
func (f *fakeFilesystem) Mount(_ context.Context, d Device, target string) error {
	f.mounts++
	f.lastDevice = d.Path
	f.ins.mu.Lock()
	defer f.ins.mu.Unlock()
	for idx := range f.ins.s.Disks {
		if f.ins.s.Disks[idx].Serial == d.Serial {
			f.ins.s.Disks[idx].Mounts = append(f.ins.s.Disks[idx].Mounts, protocol.Mount{Target: target, Source: d.Path, MajorMinor: d.MajorMinor})
		}
	}
	return nil
}
func (f *fakeFilesystem) Bind(_ context.Context, source, target string, ro bool) error {
	f.binds++
	f.ins.mu.Lock()
	defer f.ins.mu.Unlock()
	for idx := range f.ins.s.Disks {
		for _, m := range f.ins.s.Disks[idx].Mounts {
			if m.Target == source {
				if f.bindCrash {
					ro = false
				}
				f.ins.s.Disks[idx].Mounts = append(f.ins.s.Disks[idx].Mounts, protocol.Mount{Target: target, Source: m.Source, MajorMinor: m.MajorMinor, ReadOnly: ro})
				if f.bindCrash {
					return errors.New("crash after bind before RO remount")
				}
				return nil
			}
		}
	}
	return errors.New("no staging mount")
}
func (f *fakeFilesystem) Unmount(_ context.Context, target string) error {
	f.unmounts++
	f.ins.mu.Lock()
	defer f.ins.mu.Unlock()
	for idx := range f.ins.s.Disks {
		var keep []protocol.Mount
		for _, m := range f.ins.s.Disks[idx].Mounts {
			if m.Target != target {
				keep = append(keep, m)
			}
		}
		f.ins.s.Disks[idx].Mounts = keep
	}
	return nil
}
func (f *fakeFilesystem) Remount(_ context.Context, source, target string, ro bool) error {
	f.ins.mu.Lock()
	defer f.ins.mu.Unlock()
	for idx := range f.ins.s.Disks {
		for m := range f.ins.s.Disks[idx].Mounts {
			if f.ins.s.Disks[idx].Mounts[m].Target == target {
				f.ins.s.Disks[idx].Mounts[m].ReadOnly = ro
				return nil
			}
		}
	}
	return errors.New("pending bind absent")
}
func (f *fakeFilesystem) Stats(context.Context, string) (Usage, error) { return f.usage, nil }
func (f *fakeFilesystem) Expand(_ context.Context, d Device, required int64, _ int64) (int64, error) {
	if required == 0 {
		required = d.SizeBytes
	}
	f.expands++
	f.lastDevice = d.Path
	if f.expandErr != nil {
		return 0, f.expandErr
	}
	f.ins.mu.Lock()
	defer f.ins.mu.Unlock()
	for idx := range f.ins.s.Disks {
		if f.ins.s.Disks[idx].Serial == d.Serial {
			f.ins.s.Disks[idx].SizeBytes = required + 4096
		}
	}
	return required + 4096, nil
}

type fsHarness struct {
	r                             *FilesystemRuntime
	ins                           *fsInspector
	fs                            *fakeFilesystem
	j                             *Journal
	p                             protocol.StagePermit
	root, stage, targetA, targetB string
}

func newFS(t *testing.T, initialized bool) *fsHarness {
	t.Helper()
	root := t.TempDir()
	j, e := NewJournal(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	n := protocol.NodeRef{Name: "worker-a", UID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", BindingID: "node-record", ProjectID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", ServerID: "cccccccc-cccc-4ccc-8ccc-cccccccccccc", Epoch: 1}
	p := protocol.StagePermit{Node: n, Volume: protocol.VolumeRef{RecordID: "volume-record", Handle: fsHandle, ProviderID: fsVolumeUUID, Serial: "data-serial", FSUUID: fsVolumeUUID, AttachEpoch: 1}, AllowFormat: !initialized, OperationID: "format-operation", Nonce: "format-nonce", ResumeQuiesced: true}
	ins := &fsInspector{s: protocol.Snapshot{BootID: "boot-a", Disks: []protocol.Disk{{Path: "/dev/sda", Serial: "root-serial", MajorMinor: "8:0", Root: true, Partitioned: true}, {Path: "/dev/sdc", Serial: p.Volume.Serial, MajorMinor: "8:32", SizeBytes: 10 << 30}}}}
	if initialized {
		p.Initialization = "FormatComplete"
		ins.s.Disks[1].FSType = "ext4"
		ins.s.Disks[1].FSUUID = fsVolumeUUID
	}
	fs := &fakeFilesystem{ins: ins, j: j, usage: Usage{TotalBytes: 10 << 30, UsedBytes: 2 << 30, AvailableBytes: 8 << 30, TotalInodes: 1000, UsedInodes: 200, AvailableInodes: 800}}
	r := &FilesystemRuntime{Inspector: ins, Journal: j, Filesystem: fs, Paths: Paths{Root: root}, Node: func() protocol.NodeRef { return n }}
	stage := filepath.Join(root, "plugins/kubernetes.io/csi", protocol.DriverName, "pvc-a/globalmount")
	a := filepath.Join(root, "pods/99999999-9999-4999-8999-999999999999/volumes/kubernetes.io~csi/pvc-a/mount")
	b := filepath.Join(root, "pods/88888888-8888-4888-8888-888888888888/volumes/kubernetes.io~csi/pvc-a/mount")
	return &fsHarness{r, ins, fs, j, p, root, stage, a, b}
}
func (h *fsHarness) stageReady(t *testing.T) {
	t.Helper()
	result, e := h.r.Stage(context.Background(), h.p, h.stage)
	if e != nil {
		t.Fatal(e)
	}
	if result.NeedsReport {
		h.p.AllowFormat = false
		h.p.Initialization = "FormatComplete"
		if _, e = h.r.Stage(context.Background(), h.p, h.stage); e != nil {
			t.Fatal(e)
		}
	}
}
