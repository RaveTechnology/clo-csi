package coordinator

import (
	"context"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"github.com/google/uuid"
	"testing"
)

type afterCASBackend struct {
	registry.Backend
	after func(registry.Document)
}

func (b *afterCASBackend) CAS(ctx context.Context, d registry.Document, version string) (registry.Document, error) {
	out, e := b.Backend.CAS(ctx, d, version)
	if e == nil && b.after != nil {
		b.after(out)
	}
	return out, e
}

func preparedFormat(t *testing.T, h *harness) (registry.VolumeRecord, registry.OperationRecord) {
	t.Helper()
	ctx := context.Background()
	v := published(t, h)
	n, e := h.r.Node(ctx, h.node.BindingID)
	if e != nil {
		t.Fatal(e)
	}
	op, e := h.c.newOperation(ctx, "format", v, &n, h.channel.snapshot(), nil, 0)
	if e != nil {
		t.Fatal(e)
	}
	v, e = h.r.Volume(ctx, v.Name)
	if e != nil {
		t.Fatal(e)
	}
	v.Status.Initialization, v.Status.FormatOp = "FormatDispatched", op.Name
	v, e = h.r.SaveVolume(ctx, v, v.Version)
	if e != nil {
		t.Fatal(e)
	}
	return v, op
}

func TestPreparedFormatReconcileClosesPermissionBeforeReset(t *testing.T) {
	ctx := context.Background()
	b := &afterCASBackend{Backend: registry.NewMemoryBackend()}
	h := setupBackend(t, true, b)
	v, op := preparedFormat(t, h)
	reset, e := h.c.reconciliation(ctx, op)
	if e != nil {
		t.Fatal(e)
	}
	delivered := false
	b.after = func(d registry.Document) {
		if d.Kind != registry.KindVolume {
			return
		}
		fresh, err := h.r.Volume(ctx, v.Name)
		if err != nil {
			t.Fatal(err)
		}
		if fresh.Status.FormatOp != "" {
			return
		}
		b.after = nil
		g, err := h.r.Dispatch(ctx, op.Name, h.c.current())
		if err == nil {
			if err = g.Send(h.c.current(), func() error { delivered = true; return nil }); err != nil {
				t.Fatal(err)
			}
		}
	}
	if e = reset(); e != nil {
		t.Fatal(e)
	}
	fresh, e := h.r.Operation(ctx, op.Name)
	if e != nil {
		t.Fatal(e)
	}
	if delivered || fresh.Status.Sent || fresh.Status.Phase != registry.Completed {
		t.Fatal("format permission remained dispatchable after initialization reset")
	}
}

func TestCompletedUnsentFormatReconcileFinishesResetAfterRestart(t *testing.T) {
	ctx := context.Background()
	h := setup(t, true)
	v, op := preparedFormat(t, h)
	op.Status.Phase, op.Status.Result = registry.Completed, "proven not dispatched"
	if _, e := h.r.SaveOperation(ctx, op, op.Version); e != nil {
		t.Fatal(e)
	}
	if _, e := h.c.Recover(ctx, protocol.RecoveryRequest{Action: "reconcile", OperationID: op.Name, Operator: "owner", Nonce: uuid.NewString()}); e != nil {
		t.Fatal(e)
	}
	v, e := h.r.Volume(ctx, v.Name)
	if e != nil {
		t.Fatal(e)
	}
	if v.Status.FormatOp != "" || v.Status.Initialization != "" || v.Status.PendingOp != "" {
		t.Fatal("completed unsent format left initialization permanently blocked")
	}
}

func TestRecoveryCommandsCannotGuessSerialOrResendDispatchedPost(t *testing.T) {
	h := setup(t, true)
	v := create(t, h, "pvc-a")
	h.api.attachErr = context.DeadlineExceeded
	_, e := h.c.Publish(context.Background(), h.r.Handle(v.Status.ProviderID), EncodeNodeID(h.node), false)
	mustBlocked(t, e)
	v, _ = h.r.Volume(context.Background(), v.Name)
	for range 2 {
		_, e = h.c.Recover(context.Background(), protocol.RecoveryRequest{Action: "reconcile", OperationID: v.Status.PendingOp, Operator: "test", Nonce: uuid.NewString()})
		mustBlocked(t, e)
	}
	if h.api.attachCalls != 1 {
		t.Fatal("uncertain POST repeated")
	}
	v, _ = h.r.Volume(context.Background(), v.Name)
	if v.Status.Serial != "" {
		t.Fatal("serial guessed")
	}
}
func TestRetireOnlyNeverPublishedVolume(t *testing.T) {
	h := setup(t, true)
	v := published(t, h)
	_, e := h.c.Recover(context.Background(), protocol.RecoveryRequest{Action: "retire-unpublished", VolumeID: v.Name, Operator: "owner", Nonce: uuid.NewString()})
	mustBlocked(t, e)
	free := create(t, h, "unused")
	_, e = h.c.Recover(context.Background(), protocol.RecoveryRequest{Action: "retire-unpublished", VolumeID: free.Name, Operator: "owner", Nonce: uuid.NewString()})
	if e != nil {
		t.Fatal(e)
	}
	got, _ := h.r.Volume(context.Background(), free.Name)
	if !got.Status.Retired || h.api.deleteCalls != 0 {
		t.Fatal("retirement deleted provider data")
	}
}
func TestClearQuarantineRequiresCompleteInventory(t *testing.T) {
	h := setup(t, true)
	n, e := h.r.Node(context.Background(), h.node.BindingID)
	if e != nil {
		t.Fatal(e)
	}
	n.Status.Quarantined = true
	_, e = h.r.SaveNode(context.Background(), n, n.Version)
	if e != nil {
		t.Fatal(e)
	}
	h.channel.extraDisks = 1
	_, e = h.c.Recover(context.Background(), protocol.RecoveryRequest{Action: "clear-quarantine", BindingID: n.Name, Operator: "owner", Nonce: uuid.NewString()})
	mustBlocked(t, e)
	h.channel.extraDisks = 0
	_, e = h.c.Recover(context.Background(), protocol.RecoveryRequest{Action: "clear-quarantine", BindingID: n.Name, Operator: "owner", Nonce: uuid.NewString()})
	if e != nil {
		t.Fatal(e)
	}
}
func TestFormatCrashCannotCreateSecondPermit(t *testing.T) {
	h := setup(t, true)
	v := published(t, h)
	ref := protocol.VolumeRef{RecordID: v.Name, Handle: h.r.Handle(v.Status.ProviderID), ProviderID: v.Status.ProviderID, Serial: v.Status.Serial, FSUUID: v.Status.FSUUID, AttachEpoch: v.Status.AttachEpoch}
	req := protocol.StageRequest{Node: h.node, Volume: ref, Snapshot: h.channel.snapshot(), Format: true}
	permit, e := h.c.AuthorizeStage(context.Background(), req)
	if e != nil || !permit.AllowFormat {
		t.Fatalf("first permit %+v %v", permit, e)
	}
	_, e = h.c.AuthorizeStage(context.Background(), req)
	mustBlocked(t, e)
}
func TestResizeNeverRebootsOrShrinks(t *testing.T) {
	h := setup(t, true)
	v := published(t, h)
	handle := h.r.Handle(v.Status.ProviderID)
	size, e := h.c.Expand(context.Background(), handle, 12<<30, 13<<30)
	if e != nil || size != 12<<30 {
		t.Fatalf("resize=%d %v", size, e)
	}
	size, e = h.c.Expand(context.Background(), handle, 9<<30, 0)
	if e != nil || size != 12<<30 || h.api.extendCalls != 1 {
		t.Fatal("shrink/repeated extend")
	}
	if h.api.powerCalls != 0 {
		t.Fatal("resize reboot")
	}
}
func TestDeleteRequiresOwnershipNoAttachmentNoPending(t *testing.T) {
	h := setup(t, true)
	v := published(t, h)
	mustBlocked(t, h.c.Delete(context.Background(), h.r.Handle(v.Status.ProviderID)))
	if h.api.deleteCalls != 0 {
		t.Fatal("attached volume deleted")
	}
	if e := h.c.Unpublish(context.Background(), h.r.Handle(v.Status.ProviderID), ""); e != nil {
		t.Fatal(e)
	}
	if e := h.c.Delete(context.Background(), h.r.Handle(v.Status.ProviderID)); e != nil {
		t.Fatal(e)
	}
	if e := h.c.Delete(context.Background(), h.r.Handle(v.Status.ProviderID)); e != nil {
		t.Fatal(e)
	}
	if h.api.deleteCalls != 1 {
		t.Fatal("delete repeated")
	}
	if e := h.c.Delete(context.Background(), h.r.Handle(uuid.NewString())); e != nil {
		t.Fatal(e)
	}
}

func TestRecoverAmbiguousAttachmentOnlyAfterFence(t *testing.T) {
	h := setup(t, true)
	v := create(t, h, "pvc-a")
	h.api.attachHook = func() { h.channel.mu.Lock(); h.channel.extraDisks = 1; h.channel.mu.Unlock() }
	_, e := h.c.Publish(context.Background(), h.r.Handle(v.Status.ProviderID), EncodeNodeID(h.node), false)
	mustBlocked(t, e)
	v, _ = h.r.Volume(context.Background(), v.Name)
	q := protocol.RecoveryRequest{Action: "recover-attachment", OperationID: v.Status.PendingOp, Operator: "owner", Nonce: uuid.NewString()}
	_, e = h.c.Recover(context.Background(), q)
	mustBlocked(t, e)
	h.api.server.Status = "STOPPED"
	h.api.server.SwitchStatus = "OFF"
	f, e := h.c.Recover(context.Background(), protocol.RecoveryRequest{Action: "record-fence", BindingID: h.node.BindingID, Operator: "owner", Nonce: uuid.NewString()})
	if e != nil {
		t.Fatal(e)
	}
	q.FenceID = f.Name
	q.Nonce = uuid.NewString()
	_, e = h.c.Recover(context.Background(), q)
	if e != nil {
		t.Fatal(e)
	}
	got, _ := h.r.Volume(context.Background(), v.Name)
	if got.Status.Serial != "" || got.Status.PendingOp != "" || h.api.attachCalls != 1 || h.api.detachCalls != 1 || h.api.lastHot {
		t.Fatalf("unsafe fence recovery %+v", got)
	}
}
