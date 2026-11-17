package coordinator

import (
	"context"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/google/uuid"
	"testing"
)

func TestDetachNeedsCurrentQuiesceReceipt(t *testing.T) {
	h := setup(t, true)
	v := published(t, h)
	h.channel.staleBoot = true
	e := h.c.Unpublish(context.Background(), h.r.Handle(v.Status.ProviderID), EncodeNodeID(h.node))
	mustBlocked(t, e)
	if h.api.detachCalls != 0 {
		t.Fatal("stale receipt detached")
	}
}
func TestMissingReceiptBlocksUntilExplicitFence(t *testing.T) {
	h := setup(t, true)
	v := published(t, h)
	handle := h.r.Handle(v.Status.ProviderID)
	h.channel.unavailable = true
	mustBlocked(t, h.c.Unpublish(context.Background(), handle, ""))
	if h.api.detachCalls != 0 {
		t.Fatal("missing receipt detached")
	}
	h.api.server.Status = "STOPPED"
	h.api.server.SwitchStatus = "OFF"
	if _, e := h.c.Recover(context.Background(), protocol.RecoveryRequest{Action: "record-fence", BindingID: h.node.BindingID, Nonce: uuid.NewString(), Operator: "owner"}); e != nil {
		t.Fatal(e)
	}
	if e := h.c.Unpublish(context.Background(), handle, ""); e != nil {
		t.Fatal(e)
	}
	if h.api.detachCalls != 1 || h.api.lastHot || h.api.powerCalls != 0 {
		t.Fatal("manual fencing policy violated")
	}
}
func TestWrongNodeUnpublishIsNoop(t *testing.T) {
	h := setup(t, true)
	v := published(t, h)
	other := h.node
	other.UID = uuid.NewString()
	if e := h.c.Unpublish(context.Background(), h.r.Handle(v.Status.ProviderID), EncodeNodeID(other)); e != nil {
		t.Fatal(e)
	}
	if h.api.detachCalls != 0 {
		t.Fatal("foreign target detached")
	}
}
func TestEmptyNodeUnpublishUsesOwnedActualTarget(t *testing.T) {
	h := setup(t, true)
	v := published(t, h)
	if e := h.c.Unpublish(context.Background(), h.r.Handle(v.Status.ProviderID), ""); e != nil {
		t.Fatal(e)
	}
	if h.api.detachCalls != 1 || !h.api.lastHot {
		t.Fatal("owned actual target not hot detached")
	}
	if e := h.c.Unpublish(context.Background(), h.r.Handle(v.Status.ProviderID), ""); e != nil {
		t.Fatal(e)
	}
	if h.api.detachCalls != 1 {
		t.Fatal("detach repeated")
	}
}
func TestFenceRequiresExactStoppedOffOrVerifiedDeletion(t *testing.T) {
	for _, state := range [][2]string{{"ACTIVE", "OFF"}, {"STOPPED", "ON"}, {"UNKNOWN", "OFF"}, {"STOPPED", "OFF"}} {
		h := setup(t, true)
		_ = published(t, h)
		h.api.server.Status = state[0]
		h.api.server.SwitchStatus = state[1]
		_, e := h.c.Recover(context.Background(), protocol.RecoveryRequest{Action: "record-fence", BindingID: h.node.BindingID, Nonce: uuid.NewString(), Operator: "owner"})
		if (e == nil) != (state == [2]string{"STOPPED", "OFF"}) {
			t.Fatalf("fence %v result %v", state, e)
		}
		if h.api.powerCalls != 0 {
			t.Fatal("automatic VM fence")
		}
	}
}

func TestFenceCanBeRecordedBeforeMutationReadiness(t *testing.T) {
	h := setup(t, false)
	h.api.server.Status = "STOPPED"
	h.api.server.SwitchStatus = "OFF"
	if _, e := h.c.Recover(context.Background(), protocol.RecoveryRequest{Action: "record-fence", BindingID: h.node.BindingID, Nonce: uuid.NewString(), Operator: "owner"}); e != nil {
		t.Fatal(e)
	}
	if h.c.Ready(context.Background()) || h.api.powerCalls != 0 {
		t.Fatal("recording fence activated mutation or power")
	}
}
func TestDeletedFenceRequiresFreeVolumesAndProjectAccess(t *testing.T) {
	h := setup(t, true)
	v := published(t, h)
	h.api.server.ID = ""
	q := protocol.RecoveryRequest{Action: "record-fence", BindingID: h.node.BindingID, Nonce: uuid.NewString(), Operator: "owner"}
	_, e := h.c.Recover(context.Background(), q)
	mustBlocked(t, e)
	a := h.api.volumes[v.Status.ProviderID]
	a.AttachedServerID = ""
	a.Status = "AVAILABLE"
	h.api.volumes[a.ID] = a
	_, e = h.c.Recover(context.Background(), q)
	if e != nil {
		t.Fatal(e)
	}
	if e = h.c.Unpublish(context.Background(), h.r.Handle(v.Status.ProviderID), ""); e != nil {
		t.Fatal("verified deleted VM with free volume must release registered attachment:", e)
	}
	v, e = h.r.Volume(context.Background(), v.Name)
	if e != nil {
		t.Fatal(e)
	}
	if v.Status.NodeBindingID != "" || !v.Status.Barrier || v.Status.PendingOp != "" || h.api.detachCalls != 0 || h.api.powerCalls != 0 {
		t.Fatal("deleted VM release must preserve barrier and avoid provider mutation")
	}
	if e = h.c.Unpublish(context.Background(), h.r.Handle(v.Status.ProviderID), ""); e != nil {
		t.Fatal(e)
	}
}
