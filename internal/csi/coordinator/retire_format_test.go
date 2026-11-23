package coordinator

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"github.com/google/uuid"
)

func interruptedFormat(t *testing.T, h *harness) (registry.VolumeRecord, registry.OperationRecord) {
	t.Helper()
	ctx := context.Background()
	v, op := preparedFormat(t, h)
	if e := h.c.send(ctx, op, func() error { return nil }); e != nil {
		t.Fatal(e)
	}
	op, e := h.r.Operation(ctx, op.Name)
	if e != nil {
		t.Fatal(e)
	}
	return v, op
}

func TestIncompleteRetirementResumesAfterPersistedTombstoneAndCompletion(t *testing.T) {
	for _, point := range []string{"tombstone", "completion"} {
		t.Run(point, func(t *testing.T) {
			ctx := context.Background()
			b := &afterCASBackend{Backend: registry.NewMemoryBackend()}
			h := setupBackend(t, true, b)
			v, op := interruptedFormat(t, h)
			q := retirementRequest(h, v, op)
			q.FenceID = stopAndFence(t, h)
			cancelled, cancel := context.WithCancel(ctx)
			defer cancel()
			interrupted := false
			b.after = func(d registry.Document) {
				if point == "tombstone" && d.Kind == registry.KindVolume {
					var s registry.VolumeStatus
					if e := json.Unmarshal(d.Status, &s); e != nil {
						t.Fatal(e)
					}
					interrupted = s.Retired
				}
				if point == "completion" && d.Kind == registry.KindOperation {
					var s registry.OperationStatus
					if e := json.Unmarshal(d.Status, &s); e != nil {
						t.Fatal(e)
					}
					interrupted = s.Phase == registry.Completed
				}
				if interrupted {
					b.after = nil
					cancel()
				}
			}
			if _, e := h.c.Recover(cancelled, q); e == nil || !interrupted {
				t.Fatal("persistent interruption not exercised", e)
			}
			if _, e := h.c.Recover(ctx, q); e != nil {
				t.Fatal("retirement did not resume", e)
			}
			got, _ := h.r.Volume(ctx, v.Name)
			n, _ := h.r.Node(ctx, h.node.BindingID)
			closed, _ := h.r.Operation(ctx, op.Name)
			if !got.Status.Retired || got.Status.Initialization != "FormatDispatched" || got.Status.FormatOp != op.Name || got.Status.PendingOp != "" || n.Status.PendingEnrollment != "" || closed.Status.Phase != registry.Completed || !closed.Status.Sent || h.api.deleteCalls != 0 || h.api.detachCalls != 0 {
				t.Fatal("resumed retirement changed data/history or left claims")
			}
		})
	}
}

func TestIncompleteRetirementRejectsLostAuthorityAndLivePreparer(t *testing.T) {
	for _, scenario := range []string{"live-preparer", "unknown-issuer", "restore-needs-fence", "wrong-fence-epoch", "different-binding"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			b := registry.NewMemoryBackend()
			h := setupBackend(t, true, b)
			v, op := interruptedFormat(t, h)
			q := retirementRequest(h, v, op)
			q.FenceID = stopAndFence(t, h)
			switch scenario {
			case "live-preparer":
				defer h.c.trackPreparation(op.Name)()
			case "unknown-issuer":
				i, _ := h.r.Issuer(ctx)
				i.Status.Current.ProcessNonce = uuid.NewString()
				h.c.mu.Lock()
				h.c.issuer = i.Status.Current
				h.c.mu.Unlock()
				d, _ := b.Get(ctx, registry.KindIssuer, "installation")
				d.Status, _ = json.Marshal(i.Status)
				if _, e := b.CAS(ctx, d, d.Version); e != nil {
					t.Fatal(e)
				}
			case "restore-needs-fence":
				i, _ := h.r.Issuer(ctx)
				i.Status.RestoreNeedsFence = true
				d, _ := b.Get(ctx, registry.KindIssuer, "installation")
				d.Status, _ = json.Marshal(i.Status)
				if _, e := b.CAS(ctx, d, d.Version); e != nil {
					t.Fatal(e)
				}
			case "wrong-fence-epoch":
				f, _ := h.r.Fence(ctx, q.FenceID)
				spec := f.Spec
				spec.Epoch++
				spec.Nonce = uuid.NewString()
				other, e := h.r.CreateFence(ctx, spec)
				if e != nil {
					t.Fatal(e)
				}
				other.Status = f.Status
				if _, e = h.r.SaveFence(ctx, other, other.Version); e != nil {
					t.Fatal(e)
				}
				n, _ := h.r.Node(ctx, h.node.BindingID)
				n.Status.FenceID = other.Name
				q.FenceID = other.Name
				if _, e = h.r.SaveNode(ctx, n, n.Version); e != nil {
					t.Fatal(e)
				}
			case "different-binding":
				v.Status.NodeBindingID = "other"
				if _, e := h.r.SaveVolume(ctx, v, v.Version); e != nil {
					t.Fatal(e)
				}
			}
			if _, e := h.c.Recover(ctx, q); e == nil {
				t.Fatal("retirement accepted invalid authority", scenario)
			}
			got, _ := h.r.Volume(ctx, v.Name)
			if got.Status.Retired || got.Status.PendingOp != op.Name || got.Status.Initialization != "FormatDispatched" || h.api.detachCalls != 0 || h.api.deleteCalls != 0 {
				t.Fatal("rejected retirement mutated data/claims")
			}
		})
	}
}
func retirementRequest(h *harness, v registry.VolumeRecord, op registry.OperationRecord) protocol.RecoveryRequest {
	return protocol.RecoveryRequest{Action: "retire-incomplete-format", OperationID: op.Name, VolumeID: h.r.Handle(v.Status.ProviderID), Operator: "owner", Nonce: uuid.NewString()}
}
func stopAndFence(t *testing.T, h *harness) string {
	t.Helper()
	h.api.server.Status = "STOPPED"
	h.api.server.SwitchStatus = "OFF"
	f, e := h.c.Recover(context.Background(), protocol.RecoveryRequest{Action: "record-fence", BindingID: h.node.BindingID, Operator: "owner", Nonce: uuid.NewString()})
	if e != nil {
		t.Fatal(e)
	}
	return f.Name
}
func TestIncompleteFormatRetirementRetainsDiskAndInitializationHistory(t *testing.T) {
	ctx := context.Background()
	h := setup(t, true)
	v, op := interruptedFormat(t, h)
	q := retirementRequest(h, v, op)
	q.FenceID = stopAndFence(t, h)
	calls := h.api.attachCalls + h.api.detachCalls + h.api.deleteCalls + h.api.extendCalls + h.api.powerCalls
	q.DryRun = true
	if _, e := h.c.Recover(ctx, q); e != nil {
		t.Fatal(e)
	}
	before, _ := h.r.Volume(ctx, v.Name)
	if before.Status.Retired || before.Status.PendingOp != op.Name {
		t.Fatal("dry run mutated retirement")
	}
	q.DryRun = false
	if _, e := h.c.Recover(ctx, q); e != nil {
		t.Fatal(e)
	}
	got, _ := h.r.Volume(ctx, v.Name)
	closed, _ := h.r.Operation(ctx, op.Name)
	node, _ := h.r.Node(ctx, h.node.BindingID)
	if !got.Status.Retired || !got.Status.Barrier || got.Status.Initialization != "FormatDispatched" || got.Status.FormatOp != op.Name || got.Status.Serial != v.Status.Serial || got.Status.FSUUID != v.Status.FSUUID || got.Status.PendingOp != "" || node.Status.PendingEnrollment != "" || !node.Status.Fenced || closed.Status.Phase != registry.Completed || !closed.Status.Sent {
		t.Fatalf("retirement rewound identity/state: %+v %+v", got.Status, closed.Status)
	}
	if calls != h.api.attachCalls+h.api.detachCalls+h.api.deleteCalls+h.api.extendCalls+h.api.powerCalls {
		t.Fatal("retirement dispatched a native mutation")
	}
	mustBlocked(t, h.c.Delete(ctx, h.r.Handle(v.Status.ProviderID)))
	if _, e := h.c.Publish(ctx, h.r.Handle(v.Status.ProviderID), EncodeNodeID(h.node), false); e == nil {
		t.Fatal("retired data became publishable")
	}
	if _, e := h.c.Recover(ctx, q); e != nil {
		t.Fatal("retirement replay failed", e)
	}
	if e := h.c.Unpublish(ctx, h.r.Handle(v.Status.ProviderID), ""); e != nil {
		t.Fatal(e)
	}
	if h.api.deleteCalls != 0 || h.api.detachCalls != 1 || h.api.lastHot || h.api.powerCalls != 0 {
		t.Fatal("retained cold detach changed/deleted data or powered VM")
	}
	if _, e := h.api.GetVolume(ctx, v.Status.ProviderID); e != nil {
		t.Fatal("native disk was not retained", e)
	}
}
func TestIncompleteFormatRetirementRejectsMissingFenceHandleAndCompletedData(t *testing.T) {
	for _, scenario := range []string{"no-fence", "wrong-fence", "wrong-handle", "initialized", "foreign-attachment", "restarted-vm"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			h := setup(t, true)
			v, op := interruptedFormat(t, h)
			q := retirementRequest(h, v, op)
			if scenario != "no-fence" {
				q.FenceID = stopAndFence(t, h)
			}
			switch scenario {
			case "wrong-fence":
				q.FenceID = "f-other"
			case "wrong-handle":
				q.VolumeID = h.r.Handle(uuid.NewString())
			case "initialized":
				fresh, _ := h.r.Volume(ctx, v.Name)
				fresh.Status.Initialization = "FormatComplete"
				if _, e := h.r.SaveVolume(ctx, fresh, fresh.Version); e != nil {
					t.Fatal(e)
				}
			case "foreign-attachment":
				h.api.mu.Lock()
				a := h.api.volumes[v.Status.ProviderID]
				a.AttachedServerID = uuid.NewString()
				h.api.volumes[a.ID] = a
				h.api.mu.Unlock()
			case "restarted-vm":
				h.api.server.Status = "ACTIVE"
				h.api.server.SwitchStatus = "ON"
			}
			_, e := h.c.Recover(ctx, q)
			mustBlocked(t, e)
			got, _ := h.r.Volume(ctx, v.Name)
			old, _ := h.r.Operation(ctx, op.Name)
			if got.Status.Retired || got.Status.PendingOp != op.Name || old.Status.Phase == registry.Completed || !old.Status.Sent || h.api.detachCalls != 0 || h.api.deleteCalls != 0 {
				t.Fatal("rejected retirement changed data/claims")
			}
		})
	}
}
