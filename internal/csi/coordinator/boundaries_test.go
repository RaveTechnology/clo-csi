package coordinator

import (
	"context"
	"github.com/RaveTechnology/clo-cloud-go/cloud"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/google/uuid"
	"testing"
)

func TestDeleteWaitsForTerminalPostcondition(t *testing.T) {
	h := setup(t, true)
	v := create(t, h, "pvc-a")
	h.api.deleteDelay = 2
	if e := h.c.Delete(context.Background(), h.r.Handle(v.Status.ProviderID)); e != nil {
		t.Fatal(e)
	}
	if h.api.deleteCalls != 1 || h.api.pendingDeleteID != "" {
		t.Fatal("delete did not wait for confirmation")
	}
}
func TestDeleteUnknownMissingIsHarmlessWhileExistingRefused(t *testing.T) {
	h := setup(t, true)
	id := uuid.NewString()
	if e := h.c.Delete(context.Background(), h.r.Handle(id)); e != nil {
		t.Fatal(e)
	}
	h.api.volumes[id] = cloud.Volume{ID: id, Name: "foreign", Status: "AVAILABLE", SizeGB: 10}
	mustBlocked(t, h.c.Delete(context.Background(), h.r.Handle(id)))
	if h.api.deleteCalls != 0 {
		t.Fatal("foreign delete")
	}
}
func TestProviderOversizeDoesNotSatisfyUpperBound(t *testing.T) {
	h := setup(t, true)
	h.api.createSizeOverride = 20
	q := request("pvc-a")
	q.LimitBytes = 12 << 30
	_, e := h.c.EnsureVolume(context.Background(), q)
	mustBlocked(t, e)
	h2 := setup(t, true)
	v := published(t, h2)
	h2.api.extendSizeOverride = 20
	_, e = h2.c.Expand(context.Background(), h2.r.Handle(v.Status.ProviderID), 12<<30, 13<<30)
	mustBlocked(t, e)
}
func TestLivePausedIssuerKeepsInFlightLock(t *testing.T) {
	h := setup(t, true)
	v := create(t, h, "pvc-a")
	entered := make(chan struct{})
	resume := make(chan struct{})
	h.api.attachHook = func() { close(entered); <-resume }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, e := h.c.Publish(ctx, h.r.Handle(v.Status.ProviderID), EncodeNodeID(h.node), false)
		done <- e
	}()
	<-entered
	cancel()
	next := New(h.r, h.api, h.channel, h.cfg)
	mustBlocked(t, next.StartIssuer(context.Background(), controllerPod, "new-process"))
	locked, _ := h.r.Volume(context.Background(), v.Name)
	if locked.Status.PendingOp == "" {
		t.Fatal("deadline released pending lock")
	}
	close(resume)
	mustBlocked(t, <-done)
	if h.api.attachCalls != 1 {
		t.Fatal("paused issuer re-dispatched")
	}
}
func TestRegistryCannotRewindCompletedFormat(t *testing.T) {
	h := setup(t, true)
	v := published(t, h)
	ref := volumeRef(h.r, v)
	s := h.channel.snapshot()
	p, e := h.c.AuthorizeStage(context.Background(), protocol.StageRequest{Node: h.node, Volume: ref, Snapshot: s, Format: true})
	if e != nil {
		t.Fatal(e)
	}
	for i := range s.Disks {
		if s.Disks[i].Serial == ref.Serial {
			s.Disks[i].FSType = "ext4"
			s.Disks[i].FSUUID = ref.FSUUID
		}
	}
	r := protocol.Receipt{JobID: p.OperationID, Nonce: p.Nonce, Type: "formatComplete", Node: h.node, Volume: ref, Snapshot: s, FilesystemChecked: true}
	if e = h.c.Receive(context.Background(), h.node, r); e != nil {
		t.Fatal(e)
	}
	v, _ = h.r.Volume(context.Background(), v.Name)
	v.Status.Initialization = ""
	v.Status.FormatOp = ""
	_, e = h.r.SaveVolume(context.Background(), v, v.Version)
	mustBlocked(t, e)
}
func TestRetireFailureDoesNotChangeExtendedVolume(t *testing.T) {
	h := setup(t, true)
	v := published(t, h)
	h.api.extendErr = context.DeadlineExceeded
	_, e := h.c.Expand(context.Background(), h.r.Handle(v.Status.ProviderID), 12<<30, 0)
	mustBlocked(t, e)
	if h.api.extendCalls != 1 {
		t.Fatal("test did not reach uncertain native expansion")
	}
	_, e = h.c.Recover(context.Background(), protocol.RecoveryRequest{Action: "retire-unpublished", VolumeID: v.Name, Operator: "test", Nonce: uuid.NewString()})
	mustBlocked(t, e)
	v, _ = h.r.Volume(context.Background(), v.Name)
	if v.Status.Retired {
		t.Fatal("rejected retire changed lifecycle")
	}
}

func TestResumePermissionRequiresCompletedRegistryAndOpenBarrier(t *testing.T) {
	h := setup(t, true)
	ctx := context.Background()
	v := published(t, h)
	ref := volumeRef(h.r, v)
	snap := h.channel.snapshot()
	request := protocol.StageRequest{Volume: ref, Node: h.node, Snapshot: snap, Format: true}
	p, e := h.c.AuthorizeStage(ctx, request)
	if e != nil {
		t.Fatal(e)
	}
	request.Format = false
	before, e := h.c.AuthorizeStage(ctx, request)
	if e != nil || before.ResumeQuiesced {
		t.Fatal("format pending allowed quiescence resume", e)
	}
	for idx := range snap.Disks {
		if snap.Disks[idx].Serial == ref.Serial {
			snap.Disks[idx].FSType = "ext4"
			snap.Disks[idx].FSUUID = ref.FSUUID
		}
	}
	if e = h.c.Receive(ctx, h.node, protocol.Receipt{JobID: p.OperationID, Nonce: p.Nonce, Type: "formatComplete", Node: h.node, Volume: ref, Snapshot: snap, FilesystemChecked: true}); e != nil {
		t.Fatal(e)
	}
	request.Snapshot = snap
	after, e := h.c.AuthorizeStage(ctx, request)
	if e != nil || !after.ResumeQuiesced {
		t.Fatal("completed current attachment cannot restage", e)
	}
	v, _ = h.r.Volume(ctx, v.Name)
	v.Status.Barrier = true
	if _, e = h.r.SaveVolume(ctx, v, v.Version); e != nil {
		t.Fatal(e)
	}
	if _, e = h.c.AuthorizeStage(ctx, request); e == nil {
		t.Fatal("registry detach barrier issued new permission")
	}
}
