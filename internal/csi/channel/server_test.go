package channel

import (
	"context"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fixedAuth struct{ caller protocol.Caller }

func (a fixedAuth) Authenticate(context.Context, string) (protocol.Caller, error) {
	return a.caller, nil
}

type bootstrapService struct {
	ref                                     protocol.NodeRef
	registered                              bool
	registerCalls, bindingCalls, stageCalls int
}

func (s *bootstrapService) Ready(context.Context) bool { return false }
func (s *bootstrapService) RegisterNode(_ context.Context, c protocol.Caller, _ protocol.NodeHello) (protocol.NodeRef, error) {
	s.registerCalls++
	s.registered = true
	return s.ref, nil
}
func (s *bootstrapService) Binding(context.Context, protocol.Caller) (protocol.NodeRef, error) {
	s.bindingCalls++
	if !s.registered {
		return protocol.NodeRef{}, protocol.Fail(protocol.NotFound, "bootstrap binding absent")
	}
	return s.ref, nil
}
func (s *bootstrapService) AuthorizeStage(context.Context, protocol.StageRequest) (protocol.StagePermit, error) {
	s.stageCalls++
	return protocol.StagePermit{}, nil
}
func (s *bootstrapService) Receive(context.Context, protocol.NodeRef, protocol.Receipt) error {
	return nil
}
func (s *bootstrapService) PollNodeJob(context.Context, protocol.NodeRef) (protocol.Job, error) {
	return protocol.Job{}, nil
}
func (s *bootstrapService) ReceiveJobReceipt(context.Context, protocol.NodeRef, protocol.Receipt) error {
	return nil
}
func TestBootstrapAuthBeforeBindingAndReadiness(t *testing.T) {
	caller := protocol.Caller{NodeName: "worker-a", NodeUID: nodeUID, PodUID: podUID}
	s := &bootstrapService{ref: protocol.NodeRef{Name: caller.NodeName, UID: nodeUID, BindingID: "node-record", Epoch: 1}}
	h := NewServer(s, fixedAuth{caller})
	r := httptest.NewRequest(http.MethodPost, "https://controller/register", strings.NewReader(`{"identity":{"schemaVersion":1,"projectID":"project","providerName":"clo-worker-attempt","attempt":"attempt"},"snapshot":{"bootID":"boot","disks":[]}}`))
	r.Header.Set("Authorization", "Bearer projected-test-token")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 || s.registerCalls != 1 || s.bindingCalls != 0 {
		t.Fatalf("bootstrap required existing binding/readiness: %d %+v", w.Code, s)
	}
	caller.NodeUID = "another-node"
	h = NewServer(s, fixedAuth{caller})
	r = httptest.NewRequest(http.MethodPost, "https://controller/stage", strings.NewReader(`{}`))
	r.Header.Set("Authorization", "Bearer projected-test-token")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code == 200 || s.stageCalls != 0 {
		t.Fatal("cross-node binding reached stage")
	}
}
