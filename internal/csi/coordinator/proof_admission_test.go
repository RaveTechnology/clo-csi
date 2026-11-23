package coordinator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/RaveTechnology/clo-cloud-go/cloud"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
)

type proofBlockingAPI struct {
	cloud.StorageAPI
	entered, release chan struct{}
	once             sync.Once
}

func (a *proofBlockingAPI) GetProject(ctx context.Context, id string) (cloud.Project, error) {
	a.once.Do(func() {
		close(a.entered)
		select {
		case <-a.release:
		case <-ctx.Done():
		}
	})
	return a.StorageAPI.GetProject(ctx, id)
}
func TestFreshProofAdmissionRejectsConcurrentWorkAndLeavesBindingUsable(t *testing.T) {
	ctx := context.Background()
	h := setup(t, true)
	v := published(t, h)
	api := &proofBlockingAPI{StorageAPI: h.api, entered: make(chan struct{}), release: make(chan struct{})}
	h.c.api = api
	request := protocol.StageRequest{Node: h.node, Volume: volumeRef(h.r, v), Snapshot: h.channel.snapshot()}
	done := make(chan error, 1)
	go func() { _, e := h.c.AuthorizeStage(ctx, request); done <- e }()
	<-api.entered
	defer func() {
		close(api.release)
		if e := <-done; e != nil {
			t.Errorf("admitted proof failed: %v", e)
		}
	}()
	result := make(chan error, 1)
	go func() { _, e := h.c.AuthorizeStage(ctx, request); result <- e }()
	select {
	case e := <-result:
		if !code(e, protocol.Conflict) {
			t.Fatalf("busy proof did not fail fast: %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("busy proof queued cloud work")
	}
	binding, e := h.c.Binding(ctx, protocol.Caller{NodeName: h.node.Name, NodeUID: h.node.UID})
	if e != nil || binding != h.node {
		t.Fatal("proof admission blocked recovery binding", e)
	}
	if e = h.c.Receive(ctx, h.node, protocol.Receipt{}); !code(e, protocol.Conflict) {
		t.Fatal("filesystem report bypassed common proof gate", e)
	}
	if _, e = h.c.RegisterNode(ctx, h.caller, h.hello); !code(e, protocol.Conflict) {
		t.Fatal("registration bypassed common proof gate", e)
	}
}

func TestProofAdmissionReleasesOnFailedProofAndAllowsFrozenBindingVerification(t *testing.T) {
	ctx := context.Background()
	h := setup(t, true)
	bad := h.hello
	bad.Identity.ProjectID = "different"
	if _, e := h.c.RegisterNode(ctx, h.caller, bad); e == nil {
		t.Fatal("invalid registration accepted")
	}
	if _, e := h.c.RegisterNode(ctx, h.caller, h.hello); e != nil {
		t.Fatal("failed registration leaked admission gate", e)
	}
	if e := h.c.Freeze(ctx); e != nil {
		t.Fatal(e)
	}
	if got, e := h.c.RegisterNode(ctx, h.caller, h.hello); e != nil || got != h.node {
		t.Fatal("frozen binding verification blocked", e)
	}
}
