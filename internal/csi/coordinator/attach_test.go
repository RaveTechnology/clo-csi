package coordinator

import (
	"context"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"github.com/google/uuid"
	"testing"
)

func TestBootstrapNodeBindingWithoutExistingBinding(t *testing.T) {
	h := setup(t, false)
	if h.node.ServerID != serverID || h.node.UID != nodeUID || h.node.BindingID == "" {
		t.Fatal("bootstrap binding missing")
	}
	h.hello.Identity.ProviderName = "wrong"
	_, e := h.c.RegisterNode(context.Background(), h.caller, h.hello)
	mustBlocked(t, e)
}
func TestIssuerRegistersBeforeAnyCloudDispatch(t *testing.T) {
	h := setup(t, false)
	_, e := h.c.EnsureVolume(context.Background(), request("pvc-a"))
	mustBlocked(t, e)
	if h.api.createCalls != 0 {
		t.Fatal("unverified process dispatched")
	}
	if e = h.c.StartIssuer(context.Background(), controllerPod, "first-process"); e != nil {
		t.Fatal(e)
	}
	_ = create(t, h, "pvc-a")
	if h.channel.locateCalls != 1 || h.api.createCalls != 1 {
		t.Fatal("issuer locate/dispatch order")
	}
}
func TestTwoUnknownAttachmentsSerializePerNode(t *testing.T) {
	h := setup(t, true)
	a := create(t, h, "pvc-a")
	b := create(t, h, "pvc-b")
	entered := make(chan struct{})
	resume := make(chan struct{})
	h.api.attachHook = func() { close(entered); <-resume }
	done := make(chan error, 1)
	go func() {
		_, e := h.c.Publish(context.Background(), h.r.Handle(a.Status.ProviderID), EncodeNodeID(h.node), false)
		done <- e
	}()
	<-entered
	_, e := h.c.Publish(context.Background(), h.r.Handle(b.Status.ProviderID), EncodeNodeID(h.node), false)
	mustBlocked(t, e)
	close(resume)
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	if h.api.attachCalls != 1 {
		t.Fatal("two attachments dispatched")
	}
}
func TestEnrollmentRestartUsesSavedBaseline(t *testing.T) {
	h := setup(t, true)
	v := create(t, h, "pvc-a")
	h.channel.observeErr = true
	_, e := h.c.Publish(context.Background(), h.r.Handle(v.Status.ProviderID), EncodeNodeID(h.node), false)
	mustBlocked(t, e)
	h.channel.observeErr = false
	h.channel.verdict = protocol.Gone
	next := New(h.r, h.api, h.channel, h.cfg)
	if e = next.StartIssuer(context.Background(), controllerPod, "new-process"); e != nil {
		t.Fatal(e)
	}
	v, e = h.r.Volume(context.Background(), v.Name)
	if e != nil {
		t.Fatal(e)
	}
	_, e = next.Recover(context.Background(), protocol.RecoveryRequest{Action: "reconcile", OperationID: v.Status.PendingOp, Operator: "test", Nonce: uuid.NewString()})
	if e != nil {
		t.Fatal(e)
	}
	v, e = h.r.Volume(context.Background(), v.Name)
	if e != nil || v.Status.Serial != "serial-"+v.Status.ProviderID || h.api.attachCalls != 1 {
		t.Fatalf("enrollment lost baseline %+v %v", v, e)
	}
}
func TestAmbiguousDeltaQuarantinesWithoutFormat(t *testing.T) {
	h := setup(t, true)
	v := create(t, h, "pvc-a")
	h.api.attachHook = func() { h.channel.mu.Lock(); h.channel.extraDisks = 1; h.channel.mu.Unlock() }
	_, e := h.c.Publish(context.Background(), h.r.Handle(v.Status.ProviderID), EncodeNodeID(h.node), false)
	mustBlocked(t, e)
	v, _ = h.r.Volume(context.Background(), v.Name)
	n, _ := h.r.Node(context.Background(), h.node.BindingID)
	if v.Status.Serial != "" || !n.Status.Quarantined {
		t.Fatal("ambiguous serial adopted")
	}
	_, e = h.c.AuthorizeStage(context.Background(), protocol.StageRequest{Node: h.node, Volume: protocol.VolumeRef{Handle: h.r.Handle(v.Status.ProviderID)}, Format: true})
	mustBlocked(t, e)
}
func TestNodeNameReuseCannotInheritBinding(t *testing.T) {
	h := setup(t, true)
	h.caller.NodeUID = uuid.NewString()
	_, e := h.c.RegisterNode(context.Background(), h.caller, h.hello)
	mustBlocked(t, e)
	if _, e = h.c.Publish(context.Background(), "bad", EncodeNodeID(h.node), false); e == nil {
		t.Fatal("invalid handle accepted")
	}
}
func TestLateOldIssuerCannotDispatchNextMutation(t *testing.T) {
	h := setup(t, true)
	h.channel.verdict = protocol.Gone
	next := New(h.r, h.api, h.channel, h.cfg)
	if e := next.StartIssuer(context.Background(), controllerPod, "replacement"); e != nil {
		t.Fatal(e)
	}
	_, e := h.c.EnsureVolume(context.Background(), request("stale"))
	mustBlocked(t, e)
	if h.api.createCalls != 0 {
		t.Fatal("stale issuer reached cloud")
	}
}
func TestLeaseExpiryIsNotIssuerFence(t *testing.T) {
	h := setup(t, true)
	v := create(t, h, "pvc-a")
	h.api.attachErr = context.DeadlineExceeded
	_, e := h.c.Publish(context.Background(), h.r.Handle(v.Status.ProviderID), EncodeNodeID(h.node), false)
	mustBlocked(t, e)
	next := New(h.r, h.api, h.channel, h.cfg)
	e = next.StartIssuer(context.Background(), controllerPod, "replacement")
	mustBlocked(t, e)
	v, _ = h.r.Volume(context.Background(), v.Name)
	op, _ := h.r.Operation(context.Background(), v.Status.PendingOp)
	if op.Status.Phase != registry.Uncertain || h.api.attachCalls != 1 {
		t.Fatal("deadline released ownership")
	}
}

func TestExistingAgentCanRestartDuringSavedEnrollment(t *testing.T) {
	ctx := context.Background()
	h := setup(t, true)
	v := create(t, h, "pvc-a")
	h.channel.observeErr = true
	_, e := h.c.Publish(ctx, h.r.Handle(v.Status.ProviderID), EncodeNodeID(h.node), false)
	mustBlocked(t, e)
	h.channel.observeErr = false
	hello := h.hello
	hello.Snapshot = h.channel.snapshot()
	ref, e := h.c.RegisterNode(ctx, h.caller, hello)
	if e != nil || ref != h.node {
		t.Fatalf("existing verified agent could not restore recovery channel: %+v %v", ref, e)
	}
	v, _ = h.r.Volume(ctx, v.Name)
	if v.Status.Serial != "" || v.Status.PendingOp == "" {
		t.Fatal("registration adopted serial or cleared intent")
	}
	_, e = h.c.Recover(ctx, protocol.RecoveryRequest{Action: "reconcile", OperationID: v.Status.PendingOp, Operator: "owner", Nonce: uuid.NewString()})
	if e != nil {
		t.Fatal(e)
	}
	v, _ = h.r.Volume(ctx, v.Name)
	if v.Status.Serial != "serial-"+v.Status.ProviderID || h.api.attachCalls != 1 {
		t.Fatal("restart lost baseline or repeated POST")
	}
}
