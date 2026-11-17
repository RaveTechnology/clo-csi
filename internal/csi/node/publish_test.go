package node

import (
	"context"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"os"
	"path/filepath"
	"testing"
)

func TestUnpublishDeletesOnlyItsEmptyOwnedMountDirectory(t *testing.T) {
	h := newFS(t, true)
	h.stageReady(t)
	ctx := context.Background()
	if e := h.r.Publish(ctx, h.p, h.stage, h.targetA, false); e != nil {
		t.Fatal(e)
	}
	if e := os.MkdirAll(h.targetB, 0750); e != nil {
		t.Fatal(e)
	}
	foreign := filepath.Join(h.targetB, "keep")
	if e := os.WriteFile(foreign, []byte("foreign"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := h.r.Unpublish(ctx, fsHandle, h.targetB); e != nil {
		t.Fatal(e)
	}
	if e := h.r.Unpublish(ctx, fsHandle, h.targetA); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(h.targetA); !os.IsNotExist(e) {
		t.Fatal("owned target directory left after successful unpublish", e)
	}
	if _, e := os.Stat(foreign); e != nil {
		t.Fatal("unowned target changed", e)
	}
	if _, e := os.Stat(h.stage); e != nil {
		t.Fatal("stage removed by target unpublish", e)
	}
	if e := h.r.Unpublish(ctx, fsHandle, h.targetA); e != nil {
		t.Fatal("repeat after directory removal failed", e)
	}
}
func TestUnpublishRefusesNonemptyDirectoryAfterUnmount(t *testing.T) {
	h := newFS(t, true)
	h.stageReady(t)
	ctx := context.Background()
	if e := h.r.Publish(ctx, h.p, h.stage, h.targetA, false); e != nil {
		t.Fatal(e)
	}
	file := filepath.Join(h.targetA, "unexpected-underlying-file")
	if e := os.WriteFile(file, []byte("preserve"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := h.r.Unpublish(ctx, fsHandle, h.targetA); e == nil {
		t.Fatal("nonempty target incorrectly accepted as removed")
	}
	if _, e := os.Stat(file); e != nil {
		t.Fatal("underlying data removed", e)
	}
	s, e := h.j.Load(fsHandle)
	if e != nil || len(s.Targets) != 1 {
		t.Fatal("failed cleanup released durable reference", e)
	}
}
func TestUnstageRemovesItsEmptyStageDirectory(t *testing.T) {
	h := newFS(t, true)
	h.stageReady(t)
	if e := h.r.Unstage(context.Background(), fsHandle, h.stage); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(h.stage); !os.IsNotExist(e) {
		t.Fatal("stage directory left after successful unstage", e)
	}
}
func TestRepeatedUnstageAfterDeviceDetachedIsNoop(t *testing.T) {
	h := newFS(t, true)
	h.stageReady(t)
	ctx := context.Background()
	if e := h.r.Unstage(ctx, fsHandle, h.stage); e != nil {
		t.Fatal(e)
	}
	h.ins.s.Disks = h.ins.s.Disks[:1]
	if e := h.r.Unstage(ctx, fsHandle, h.stage); e != nil {
		t.Fatal("already unstaged volume depends on detached device", e)
	}
}

func TestMultiplePodRefsAndReadonlyBind(t *testing.T) {
	h := newFS(t, true)
	h.stageReady(t)
	ctx := context.Background()
	if e := h.r.Publish(ctx, h.p, h.stage, h.targetA, false); e != nil {
		t.Fatal(e)
	}
	if e := h.r.Publish(ctx, h.p, h.stage, h.targetB, true); e != nil {
		t.Fatal(e)
	}
	s, e := h.j.Load(fsHandle)
	if e != nil || len(s.Targets) != 2 || s.Targets[h.targetA] || !s.Targets[h.targetB] {
		t.Fatal("mixed RO/RW refs lost", e)
	}
	snapshot, _ := h.ins.Snapshot(ctx)
	ro := false
	for _, d := range snapshot.Disks {
		for _, m := range d.Mounts {
			if m.Target == h.targetB {
				ro = m.ReadOnly
			}
		}
	}
	if !ro {
		t.Fatal("actual bind not read-only")
	}
}
func TestAlreadyMountedWrongSourceOrReadonlyReturnsAlreadyExists(t *testing.T) {
	for _, which := range []string{"source", "readonly"} {
		t.Run(which, func(t *testing.T) {
			h := newFS(t, true)
			h.stageReady(t)
			if e := h.r.Publish(context.Background(), h.p, h.stage, h.targetA, false); e != nil {
				t.Fatal(e)
			}
			if which == "source" {
				h.ins.s.Disks[1].Mounts[1].MajorMinor = "8:0"
			} else {
				h.ins.s.Disks[1].Mounts[1].ReadOnly = true
			}
			if e := h.r.Publish(context.Background(), h.p, h.stage, h.targetA, false); e == nil {
				t.Fatal("incompatible existing mount accepted")
			}
			if h.fs.binds != 1 {
				t.Fatal("existing mount overwritten")
			}
		})
	}
}
func TestUnstageRequiresZeroRefsAndPersistsBarrierReceipt(t *testing.T) {
	h := newFS(t, true)
	h.stageReady(t)
	ctx := context.Background()
	if e := h.r.Publish(ctx, h.p, h.stage, h.targetA, false); e != nil {
		t.Fatal(e)
	}
	if e := h.r.Unstage(ctx, fsHandle, h.stage); e == nil {
		t.Fatal("active refs unmounted")
	}
	if e := h.r.Unpublish(ctx, fsHandle, h.targetA); e != nil {
		t.Fatal(e)
	}
	if e := h.r.Unstage(ctx, fsHandle, h.stage); e != nil {
		t.Fatal(e)
	}
	s, e := h.j.Load(fsHandle)
	if e != nil || !s.Barrier || s.StagePath != "" || s.DetachReceipt == nil || !s.DetachReceipt.Quiesced || s.DetachReceipt.Snapshot.BootID != "boot-a" {
		t.Fatalf("barrier receipt missing %+v %v", s, e)
	}
}
func TestOldAttachmentEpochCannotRestage(t *testing.T) {
	h := newFS(t, true)
	h.stageReady(t)
	ctx := context.Background()
	if e := h.r.Unstage(ctx, fsHandle, h.stage); e != nil {
		t.Fatal(e)
	}
	agent := &Runtime{Inspector: h.ins, Journal: h.j, Node: h.r.Node, Filesystem: h.fs}
	job := protocol.Job{ID: "quiesce", Nonce: "nonce", Type: "quiesce", Node: h.p.Node, Volume: h.p.Volume}
	if _, e := agent.Execute(ctx, job); e != nil {
		t.Fatal(e)
	}
	old := h.p
	h.p.Volume.AttachEpoch++
	if _, e := h.r.Stage(ctx, h.p, h.stage); e != nil {
		t.Fatal(e)
	}
	if _, e := h.r.Stage(ctx, old, h.stage); e == nil {
		t.Fatal("old generation staged")
	}
}
func TestRestageBeforeDetachAndCommittedBarrier(t *testing.T) {
	h := newFS(t, true)
	h.stageReady(t)
	ctx := context.Background()
	if e := h.r.Unstage(ctx, fsHandle, h.stage); e != nil {
		t.Fatal(e)
	}
	if _, e := h.r.Stage(ctx, h.p, h.stage); e != nil {
		t.Fatal("normal same-attachment restage deadlocked", e)
	}
	if e := h.r.Unstage(ctx, fsHandle, h.stage); e != nil {
		t.Fatal(e)
	}
	agent := &Runtime{Inspector: h.ins, Journal: h.j, Node: h.r.Node, Filesystem: h.fs}
	if _, e := agent.Execute(ctx, protocol.Job{ID: "quiesce", Nonce: "nonce", Type: "quiesce", Node: h.p.Node, Volume: h.p.Volume}); e != nil {
		t.Fatal(e)
	}
	if _, e := h.r.Stage(ctx, h.p, h.stage); e == nil {
		t.Fatal("stale permit crossed committed detach barrier")
	}
}
func TestUnpublishRemovesOnlyOwnedTarget(t *testing.T) {
	h := newFS(t, true)
	h.stageReady(t)
	ctx := context.Background()
	if e := h.r.Publish(ctx, h.p, h.stage, h.targetA, false); e != nil {
		t.Fatal(e)
	}
	if e := h.r.Unpublish(ctx, fsHandle, h.targetB); e != nil {
		t.Fatal(e)
	}
	if h.fs.unmounts != 0 {
		t.Fatal("unowned target unmounted")
	}
	if e := h.r.Unpublish(ctx, fsHandle, h.targetA); e != nil {
		t.Fatal(e)
	}
	if e := h.r.Unpublish(ctx, fsHandle, h.targetA); e != nil || h.fs.unmounts != 1 {
		t.Fatal("unpublish not idempotent", e)
	}
}
func TestNodeStatsAndOnlineResizeUseActualDevice(t *testing.T) {
	h := newFS(t, true)
	h.stageReady(t)
	ctx := context.Background()
	usage, e := h.r.Stats(ctx, fsHandle, h.stage)
	if e != nil || usage.AvailableBytes != 8<<30 || usage.UsedInodes != 200 {
		t.Fatal("actual stats lost", e)
	}
	size, e := h.r.Expand(ctx, h.p, h.stage, 12<<30, 0)
	if e != nil || size < 12<<30 || h.fs.lastDevice != "/dev/sdc" {
		t.Fatal("wrong resize device", e)
	}
	if _, e = h.r.Stats(ctx, fsHandle, h.targetB); e == nil {
		t.Fatal("unmounted target stats accepted")
	}
}

func TestReadonlyBindCrashResumesWithoutStacking(t *testing.T) {
	h := newFS(t, true)
	h.stageReady(t)
	h.fs.bindCrash = true
	if e := h.r.Publish(context.Background(), h.p, h.stage, h.targetA, true); e == nil {
		t.Fatal("bind crash missed")
	}
	h.fs.bindCrash = false
	if e := h.r.Publish(context.Background(), h.p, h.stage, h.targetA, true); e != nil {
		t.Fatal("saved own RO intent could not resume", e)
	}
	if h.fs.binds != 1 {
		t.Fatal("bind mount stacked on retry")
	}
}

func TestMountBeforeQuiescePreventsDetachReceipt(t *testing.T) {
	h := newFS(t, true)
	h.stageReady(t)
	ctx := context.Background()
	if e := h.r.Unstage(ctx, fsHandle, h.stage); e != nil {
		t.Fatal(e)
	}
	if _, e := h.r.Stage(ctx, h.p, h.stage); e != nil {
		t.Fatal(e)
	}
	agent := &Runtime{Inspector: h.ins, Journal: h.j, Node: h.r.Node, Filesystem: h.fs}
	job := protocol.Job{ID: "quiesce-before-unmount", Nonce: "nonce", Type: "quiesce", Node: h.p.Node, Volume: h.p.Volume}
	receipt, e := agent.Execute(ctx, job)
	if e == nil || receipt.Quiesced {
		t.Fatal("mounted attachment received detach proof")
	}
	if e = h.r.Unstage(ctx, fsHandle, h.stage); e != nil {
		t.Fatal(e)
	}
	job.ID = "quiesce-after-unmount"
	receipt, e = agent.Execute(ctx, job)
	if e != nil || !receipt.Quiesced || receipt.Snapshot.BootID != "boot-a" {
		t.Fatal("unmounted current boot did not receive proof", e)
	}
}
