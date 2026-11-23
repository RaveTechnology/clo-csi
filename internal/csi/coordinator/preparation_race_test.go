package coordinator

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"github.com/google/uuid"
)

type preparationBackend struct {
	registry.Backend
	after func(registry.Document)
}

func (b *preparationBackend) Create(ctx context.Context, d registry.Document) (registry.Document, error) {
	out, e := b.Backend.Create(ctx, d)
	if e == nil && b.after != nil {
		b.after(out)
	}
	return out, e
}
func (b *preparationBackend) CAS(ctx context.Context, d registry.Document, version string) (registry.Document, error) {
	out, e := b.Backend.CAS(ctx, d, version)
	if e == nil && b.after != nil {
		b.after(out)
	}
	return out, e
}

func TestRecoveryCannotCloseLivePreparation(t *testing.T) {
	for _, point := range []string{"created", "node-claimed", "volume-claimed", "format-marker"} {
		t.Run(point, func(t *testing.T) {
			ctx := context.Background()
			b := &preparationBackend{Backend: registry.NewMemoryBackend()}
			h := setupBackend(t, true, b)
			v := published(t, h)
			called := false
			b.after = func(d registry.Document) {
				name := ""
				if point == "created" && d.Kind == registry.KindOperation {
					name = d.Name
				}
				if point == "node-claimed" && d.Kind == registry.KindNode {
					n, e := h.r.Node(ctx, d.Name)
					if e != nil {
						t.Fatal(e)
					}
					name = n.Status.PendingEnrollment
				}
				if d.Kind == registry.KindVolume && (point == "volume-claimed" || point == "format-marker") {
					fresh, e := h.r.Volume(ctx, d.Name)
					if e != nil {
						t.Fatal(e)
					}
					if point == "volume-claimed" && fresh.Status.Initialization == "" {
						name = fresh.Status.PendingOp
					}
					if point == "format-marker" && fresh.Status.Initialization == "FormatDispatched" {
						name = fresh.Status.FormatOp
					}
				}
				if name == "" {
					return
				}
				b.after = nil
				called = true
				_, e := h.c.Recover(ctx, protocol.RecoveryRequest{Action: "reconcile", OperationID: name, Operator: "owner", Nonce: uuid.NewString()})
				if !code(e, protocol.Conflict) {
					t.Errorf("recovery overtook live preparation: %v", e)
				}
			}
			permit, e := h.c.AuthorizeStage(ctx, protocol.StageRequest{Node: h.node, Volume: volumeRef(h.r, v), Snapshot: h.channel.snapshot(), Format: true})
			if e != nil {
				t.Fatal(e)
			}
			if !called || !permit.AllowFormat {
				t.Fatal("race not exercised or live format failed")
			}
			op, _ := h.r.Operation(ctx, permit.OperationID)
			fresh, _ := h.r.Volume(ctx, v.Name)
			node, _ := h.r.Node(ctx, h.node.BindingID)
			if op.Status.Phase != registry.Dispatched || !op.Status.Sent || fresh.Status.PendingOp != op.Name || fresh.Status.FormatOp != op.Name || node.Status.PendingEnrollment != op.Name {
				t.Fatal("live preparation metadata changed during recovery")
			}
		})
	}
}

func TestCancelledPreparationCleanupRejectsLostAuthority(t *testing.T) {
	for _, condition := range []string{"restore-needs-fence", "different-issuer", "ordinary-freeze"} {
		t.Run(condition, func(t *testing.T) {
			ctx := context.Background()
			backend := registry.NewMemoryBackend()
			h := setupBackend(t, true, backend)
			v := published(t, h)
			n, _ := h.r.Node(ctx, h.node.BindingID)
			op, e := h.r.CreateOperation(ctx, registry.OperationSpec{Type: "format", VolumeRecordID: v.Name, NodeBindingID: n.Name, Nonce: uuid.NewString(), Issuer: h.c.current()})
			if e != nil {
				t.Fatal(e)
			}
			if e = h.r.LockNode(ctx, n.Name, op.Name, n.Version); e != nil {
				t.Fatal(e)
			}
			document, e := backend.Get(ctx, registry.KindIssuer, "installation")
			if e != nil {
				t.Fatal(e)
			}
			epoch, e := h.r.Issuer(ctx)
			if e != nil {
				t.Fatal(e)
			}
			if condition == "restore-needs-fence" {
				epoch.Status.Frozen = true
				epoch.Status.RestoreNeedsFence = true
			} else if condition == "different-issuer" {
				epoch.Status.Current.ProcessNonce = uuid.NewString()
			} else {
				epoch.Status.Frozen = true
			}
			document.Status, e = json.Marshal(epoch.Status)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = backend.CAS(ctx, document, document.Version); e != nil {
				t.Fatal(e)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			h.c.abortPrepared(cancelled, op)
			fresh, _ := h.r.Operation(ctx, op.Name)
			node, _ := h.r.Node(ctx, n.Name)
			if condition == "ordinary-freeze" {
				if fresh.Status.Phase != registry.Completed || fresh.Status.Sent || node.Status.PendingEnrollment != "" {
					t.Fatal("ordinary freeze prevented admitted preparation cleanup")
				}
			} else if fresh.Status.Phase != registry.Prepared || fresh.Status.Sent || node.Status.PendingEnrollment != op.Name {
				t.Fatal("cancelled cleanup changed fenced/foreign issuer state")
			}
		})
	}
}
