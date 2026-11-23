package coordinator

import (
	"context"
	"testing"

	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"github.com/google/uuid"
)

func TestReconcileUnsentPartiallyClaimedOperation(t *testing.T) {
	for _, claim := range []string{"node", "volume"} {
		t.Run(claim, func(t *testing.T) {
			ctx := context.Background()
			h := setup(t, true)
			v := published(t, h)
			n, err := h.r.Node(ctx, h.node.BindingID)
			if err != nil {
				t.Fatal(err)
			}
			op, err := h.r.CreateOperation(ctx, registry.OperationSpec{Type: "format", VolumeRecordID: v.Name, NodeBindingID: n.Name, Nonce: uuid.NewString(), Issuer: h.c.current()})
			if err != nil {
				t.Fatal(err)
			}
			// A crash between the two independent claims left only one acquired.
			if claim == "node" {
				err = h.r.LockNode(ctx, n.Name, op.Name, n.Version)
			} else {
				err = h.r.LockVolume(ctx, v.Name, op.Name, v.Version)
			}
			if err != nil {
				t.Fatal(err)
			}
			nativeCalls := h.api.attachCalls + h.api.detachCalls + h.api.createCalls + h.api.extendCalls + h.api.deleteCalls
			q := protocol.RecoveryRequest{Action: "reconcile", OperationID: op.Name, Operator: "owner", Nonce: uuid.NewString(), DryRun: true}
			if _, err = h.c.Recover(ctx, q); err != nil {
				t.Fatal(err)
			}
			old, _ := h.r.Operation(ctx, op.Name)
			if old.Status.Phase != registry.Prepared {
				t.Fatal("dry run changed operation")
			}
			q.DryRun = false
			if _, err = h.c.Recover(ctx, q); err != nil {
				t.Fatal(err)
			}
			got, _ := h.r.Operation(ctx, op.Name)
			node, _ := h.r.Node(ctx, n.Name)
			volume, _ := h.r.Volume(ctx, v.Name)
			if got.Status.Phase != registry.Completed || got.Status.Sent || got.Status.Result != "proven not dispatched" || node.Status.PendingEnrollment != "" || volume.Status.PendingOp != "" {
				t.Fatalf("partial claim remained: op=%+v node=%+v volume=%+v", got.Status, node.Status, volume.Status)
			}
			if nativeCalls != h.api.attachCalls+h.api.detachCalls+h.api.createCalls+h.api.extendCalls+h.api.deleteCalls {
				t.Fatal("reconciliation dispatched a native mutation")
			}
			if _, err = h.r.Dispatch(ctx, op.Name, h.c.current()); err == nil {
				t.Fatal("closed unsent operation became dispatchable")
			}
		})
	}
}

func TestPartialUnsentReconcilePreservesForeignClaim(t *testing.T) {
	ctx := context.Background()
	h := setup(t, true)
	v := published(t, h)
	n, _ := h.r.Node(ctx, h.node.BindingID)
	op, err := h.r.CreateOperation(ctx, registry.OperationSpec{Type: "format", VolumeRecordID: v.Name, NodeBindingID: n.Name, Nonce: uuid.NewString(), Issuer: h.c.current()})
	if err != nil {
		t.Fatal(err)
	}
	if err = h.r.LockNode(ctx, n.Name, op.Name, n.Version); err != nil {
		t.Fatal(err)
	}
	foreign, err := h.r.CreateOperation(ctx, registry.OperationSpec{Type: "format", VolumeRecordID: v.Name, Nonce: uuid.NewString(), Issuer: h.c.current()})
	if err != nil {
		t.Fatal(err)
	}
	if err = h.r.LockVolume(ctx, v.Name, foreign.Name, v.Version); err != nil {
		t.Fatal(err)
	}
	if _, err = h.c.Recover(ctx, protocol.RecoveryRequest{Action: "reconcile", OperationID: op.Name, Operator: "owner", Nonce: uuid.NewString()}); err != nil {
		t.Fatal(err)
	}
	volume, _ := h.r.Volume(ctx, v.Name)
	node, _ := h.r.Node(ctx, n.Name)
	other, _ := h.r.Operation(ctx, foreign.Name)
	if volume.Status.PendingOp != foreign.Name || node.Status.PendingEnrollment != "" || other.Status.Phase != registry.Prepared || other.Status.Sent {
		t.Fatal("foreign operation/claim changed")
	}
}

func TestDispatchStillRequiresBothClaims(t *testing.T) {
	ctx := context.Background()
	h := setup(t, true)
	v := published(t, h)
	n, _ := h.r.Node(ctx, h.node.BindingID)
	op, err := h.r.CreateOperation(ctx, registry.OperationSpec{Type: "format", VolumeRecordID: v.Name, NodeBindingID: n.Name, Nonce: uuid.NewString(), Issuer: h.c.current()})
	if err != nil {
		t.Fatal(err)
	}
	if err = h.r.LockNode(ctx, n.Name, op.Name, n.Version); err != nil {
		t.Fatal(err)
	}
	if err = h.c.send(ctx, op, func() error { t.Fatal("partial operation dispatched"); return nil }); err == nil {
		t.Fatal("partial authority accepted")
	}
	got, _ := h.r.Operation(ctx, op.Name)
	if got.Status.Sent || got.Status.Phase != registry.Prepared {
		t.Fatal("missing claim consumed dispatch")
	}
}

func TestAbortingPartialPreparationSurvivesCallerCancellation(t *testing.T) {
	ctx := context.Background()
	h := setup(t, true)
	v := published(t, h)
	n, _ := h.r.Node(ctx, h.node.BindingID)
	op, err := h.r.CreateOperation(ctx, registry.OperationSpec{Type: "format", VolumeRecordID: v.Name, NodeBindingID: n.Name, Nonce: uuid.NewString(), Issuer: h.c.current()})
	if err != nil {
		t.Fatal(err)
	}
	if err = h.r.LockNode(ctx, n.Name, op.Name, n.Version); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	h.c.abortPrepared(cancelled, op)
	fresh, _ := h.r.Operation(ctx, op.Name)
	node, _ := h.r.Node(ctx, n.Name)
	if fresh.Status.Phase != registry.Completed || fresh.Status.Sent || node.Status.PendingEnrollment != "" {
		t.Fatal("cancelled request leaked partial preparation claim")
	}
}
