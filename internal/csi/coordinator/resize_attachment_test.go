package coordinator

import (
	"context"
	"testing"

	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
)

func TestDetachedGrowthRejectedBeforeCreatingOperationOrCallingCLO(t *testing.T) {
	h := setup(t, true)
	v := create(t, h, "detached-grow")
	before, err := h.r.Operations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	size, err := h.c.Expand(context.Background(), h.r.Handle(v.Status.ProviderID), 12<<30, 0)
	if !code(err, protocol.Blocked) || size != 0 || h.api.extendCalls != 0 {
		t.Fatalf("detached growth must fail before native dispatch: size=%d err=%v POSTs=%d", size, err, h.api.extendCalls)
	}
	after, err := h.r.Operations(context.Background())
	if err != nil || len(after) != len(before) {
		t.Fatalf("rejected request created an operation: %d -> %d, %v", len(before), len(after), err)
	}
	current, _ := h.r.Volume(context.Background(), v.Name)
	if current.Status.PendingOp != "" {
		t.Fatal("rejected request claimed the volume")
	}
}

func TestGrowthRequiresConsistentLiveRegisteredAttachment(t *testing.T) {
	for _, scenario := range []string{"free-status", "missing-server", "other-server", "missing-binding", "barrier", "unpublished", "fenced", "quarantined", "server-off"} {
		t.Run(scenario, func(t *testing.T) {
			h := setup(t, true)
			ctx := context.Background()
			var v registry.VolumeRecord
			if scenario == "unpublished" {
				v = create(t, h, "pending-enrollment")
				if err := h.api.AttachVolume(ctx, v.Status.ProviderID, serverID); err != nil {
					t.Fatal(err)
				}
				v.Status.NodeBindingID, v.Status.Serial = h.node.BindingID, "pending-serial"
				var err error
				v, err = h.r.SaveVolume(ctx, v, v.Version)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				v = published(t, h)
			}
			switch scenario {
			case "free-status", "missing-server", "other-server":
				h.api.mu.Lock()
				a := h.api.volumes[v.Status.ProviderID]
				if scenario == "free-status" {
					a.Status = "AVAILABLE"
				}
				if scenario == "missing-server" {
					a.AttachedServerID = ""
				}
				if scenario == "other-server" {
					a.AttachedServerID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
				}
				h.api.volumes[a.ID] = a
				h.api.mu.Unlock()
			case "missing-binding", "barrier", "unpublished":
				if scenario == "missing-binding" {
					v.Status.NodeBindingID = ""
				}
				if scenario == "barrier" {
					v.Status.Barrier = true
				}
				if _, err := h.r.SaveVolume(ctx, v, v.Version); err != nil {
					t.Fatal(err)
				}
			case "fenced", "quarantined":
				n, err := h.r.Node(ctx, h.node.BindingID)
				if err != nil {
					t.Fatal(err)
				}
				n.Status.Fenced = scenario == "fenced"
				n.Status.Quarantined = scenario == "quarantined"
				if _, err := h.r.SaveNode(ctx, n, n.Version); err != nil {
					t.Fatal(err)
				}
			case "server-off":
				h.api.mu.Lock()
				h.api.server.Status = "STOPPED"
				h.api.server.SwitchStatus = "OFF"
				h.api.mu.Unlock()
			}
			size, err := h.c.Expand(ctx, h.r.Handle(v.Status.ProviderID), 12<<30, 0)
			if !code(err, protocol.Blocked) || size != 0 || h.api.extendCalls != 0 {
				t.Fatalf("inconsistent attachment reached expansion: size=%d err=%v POSTs=%d", size, err, h.api.extendCalls)
			}
		})
	}
}

func TestGrowthRechecksAttachmentAfterClaimBeforeNativeDispatch(t *testing.T) {
	b := claimBackend(func(s registry.OperationSpec) bool { return s.Type == "extend" })
	h := setupBackend(t, true, b)
	v := published(t, h)
	b.armed.Store(true)
	done := make(chan error, 1)
	go func() {
		_, err := h.c.Expand(context.Background(), h.r.Handle(v.Status.ProviderID), 12<<30, 0)
		done <- err
	}()
	waitClaim(t, b)
	h.api.mu.Lock()
	a := h.api.volumes[v.Status.ProviderID]
	a.Status = "AVAILABLE"
	a.AttachedServerID = ""
	h.api.volumes[a.ID] = a
	h.api.mu.Unlock()
	close(b.resume)
	err := <-done
	if !code(err, protocol.Blocked) || h.api.extendCalls != 0 {
		t.Fatalf("detached after preflight: err=%v POSTs=%d", err, h.api.extendCalls)
	}
	current, _ := h.r.Volume(context.Background(), v.Name)
	node, _ := h.r.Node(context.Background(), h.node.BindingID)
	if current.Status.PendingOp != "" || node.Status.PendingEnrollment != "" {
		t.Fatal("unsent rejected grow left claims held")
	}
}

func TestAttachedGrowthLocksNodeAgainstConcurrentDetach(t *testing.T) {
	h := setup(t, true)
	v := published(t, h)
	ctx := context.Background()
	op, err := h.c.newOperation(ctx, "detach", v, nil, protocol.Snapshot{}, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	h.c.abortPrepared(ctx, op)
	n, err := h.r.Node(ctx, h.node.BindingID)
	if err != nil {
		t.Fatal(err)
	}
	// A node-level enrollment claimed by another operation must prevent growth.
	other, err := h.r.CreateOperation(ctx, registry.OperationSpec{Type: "attach", Nonce: "busy-node", Issuer: h.c.current(), NodeBindingID: n.Name})
	if err != nil {
		t.Fatal(err)
	}
	if err = h.r.LockNode(ctx, n.Name, other.Name, n.Version); err != nil {
		t.Fatal(err)
	}
	size, err := h.c.Expand(ctx, h.r.Handle(v.Status.ProviderID), 12<<30, 0)
	if err == nil || size != 0 || h.api.extendCalls != 0 {
		t.Fatalf("busy node expanded: size=%d err=%v POSTs=%d", size, err, h.api.extendCalls)
	}
}

func TestDetachedSatisfiedCapacityRemainsIdempotentWithoutMutation(t *testing.T) {
	h := setup(t, true)
	v := create(t, h, "detached-already-sized")
	size, err := h.c.Expand(context.Background(), h.r.Handle(v.Status.ProviderID), 10<<30, 0)
	if err != nil || size != 10<<30 || h.api.extendCalls != 0 {
		t.Fatalf("no-op capacity observation mutated volume: size=%d err=%v POSTs=%d", size, err, h.api.extendCalls)
	}
}
