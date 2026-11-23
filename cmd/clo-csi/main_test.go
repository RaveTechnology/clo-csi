package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"io"
	"strings"
	"testing"
)

const testProject = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
const testInstall = "11111111-1111-4111-8111-111111111111"
const testNonce = "99999999-9999-4999-8999-999999999999"

type adminSpy struct {
	calls    int
	requests []AdminRequest
}

func (s *adminSpy) Do(_ context.Context, r AdminRequest) (AdminResponse, error) {
	s.calls++
	s.requests = append(s.requests, r)
	return AdminResponse{Message: "checked"}, nil
}
func argsFor(action string) []string {
	return []string{"admin", action, "--context=clo-lab", "--project-id=" + testProject, "--installation-id=" + testInstall, "--namespace=clo-system", "--operator=owner", "--nonce=" + testNonce, "--operation=o-test", "--binding=n-test", "--volume=v-test", "--fence=f-test"}
}
func TestCLIRequiresExplicitScopeForWrites(t *testing.T) {
	for _, action := range []string{"reconcile", "record-fence", "recover-attachment", "clear-quarantine", "retire-unpublished", "retire-incomplete-format", "restore", "freeze", "resume"} {
		for _, missing := range []string{"context", "project-id", "installation-id"} {
			t.Run(action+"-"+missing, func(t *testing.T) {
				spy := &adminSpy{}
				args := argsFor(action)
				filtered := []string{}
				for _, s := range args {
					if !strings.HasPrefix(s, "--"+missing+"=") {
						filtered = append(filtered, s)
					}
				}
				if e := run(context.Background(), filtered, strings.NewReader(""), io.Discard, Dependencies{Admin: spy}); e == nil {
					t.Fatal("unscoped write accepted")
				}
				if spy.calls != 0 {
					t.Fatal("unscoped request reached coordinator transport")
				}
			})
		}
	}
}
func TestAdminDryRunHasNoMutation(t *testing.T) {
	for _, action := range []string{"reconcile", "record-fence", "recover-attachment", "clear-quarantine", "retire-unpublished", "retire-incomplete-format"} {
		spy := &adminSpy{}
		args := append(argsFor(action), "--dry-run")
		if e := run(context.Background(), args, strings.NewReader(""), io.Discard, Dependencies{Admin: spy}); e != nil {
			t.Fatal(e)
		}
		if spy.calls != 1 || !spy.requests[0].Recovery.DryRun || spy.requests[0].Recovery.Operator != "owner" || spy.requests[0].Scope.ProjectID != testProject {
			t.Fatal("dry run was not forwarded intact")
		}
	}
}
func TestControllerNonceAndSecretRedaction(t *testing.T) {
	initial := []string{"controller", "--context=in-cluster", "--project-id=" + testProject, "--installation-id=" + testInstall}
	args, e := controllerArgv(initial, func() (string, error) { return testNonce, nil })
	if e != nil {
		t.Fatal(e)
	}
	count := 0
	for _, arg := range args {
		if arg == "--process-nonce="+testNonce {
			count++
		}
	}
	if count != 1 {
		t.Fatal("nonce not in actual argv")
	}
	again, e := controllerArgv(args, func() (string, error) { t.Fatal("saved argv nonce regenerated"); return "", nil })
	if e != nil || strings.Join(again, " ") != strings.Join(args, " ") {
		t.Fatal("exec argv not stable")
	}
	output := &bytes.Buffer{}
	launch := func(Config) error { return errors.New("do-not-echo-credential") }
	e = run(context.Background(), append(args, "--node-name=worker-a"), strings.NewReader(""), output, Dependencies{Controller: launch})
	if e == nil || strings.Contains(e.Error(), "do-not-echo-credential") || strings.Contains(output.String(), "do-not-echo-credential") {
		t.Fatal("controller error exposed credentials")
	}
	if e = run(context.Background(), []string{"controller", "--token=do-not-echo-credential"}, strings.NewReader(""), output, Dependencies{Controller: launch}); e == nil || strings.Contains(e.Error(), "do-not-echo-credential") {
		t.Fatal("parser exposed unknown secret argument")
	}
}

type maintenanceSpy struct {
	repo          *registry.Repository
	calls, writes int
	last          protocol.RecoveryRequest
}

func (s *maintenanceSpy) Repository() *registry.Repository { return s.repo }
func (s *maintenanceSpy) Recover(_ context.Context, q protocol.RecoveryRequest) (registry.OperationRecord, error) {
	s.calls++
	s.last = q
	if !q.DryRun {
		s.writes++
	}
	return registry.OperationRecord{Meta: registry.Meta{Name: q.OperationID}, Status: registry.OperationStatus{Phase: registry.Prepared, Result: "coordinator"}}, nil
}
func (s *maintenanceSpy) Maintain(_ context.Context, q protocol.RecoveryRequest, _ []byte) ([]byte, error) {
	if !q.DryRun && q.Action != "export" {
		s.writes++
	}
	return []byte("backup"), nil
}
func TestAdminRecoveryUsesCoordinator(t *testing.T) {
	r := registry.New(registry.NewMemoryBackend(), registry.Installation{ID: testInstall, ProjectID: testProject})
	coordinator := &maintenanceSpy{repo: r}
	scope := Scope{Context: "clo-lab", ProjectID: testProject, InstallationID: testInstall, Namespace: "clo-system"}
	service := NewAdminService(coordinator, scope)
	q := AdminRequest{Scope: scope, Action: "reconcile", Recovery: protocol.RecoveryRequest{Action: "reconcile", OperationID: "o-test", Operator: "owner", Nonce: testNonce, DryRun: true}}
	if _, e := service.Do(context.Background(), q); e != nil {
		t.Fatal(e)
	}
	if coordinator.calls != 1 || coordinator.writes != 0 || coordinator.last.Nonce != testNonce {
		t.Fatal("admin bypassed coordinator/dry run")
	}
	q.Scope.ProjectID = "other"
	if _, e := service.Do(context.Background(), q); e == nil {
		t.Fatal("cross-project admin accepted")
	}
	if coordinator.calls != 1 {
		t.Fatal("wrong scope reached coordinator")
	}
}

func TestAdminFenceResponseUsesPersistedFence(t *testing.T) {
	ctx := context.Background()
	r := registry.New(registry.NewMemoryBackend(), registry.Installation{ID: testInstall, ProjectID: testProject})
	f, e := r.CreateFence(ctx, registry.FenceSpec{BindingID: "n-test", ServerID: "server", NodeUID: "node", Epoch: 1, Operator: "owner", Nonce: testNonce})
	if e != nil {
		t.Fatal(e)
	}
	scope := Scope{Context: "clo-lab", ProjectID: testProject, InstallationID: testInstall, Namespace: "clo-system"}
	s := NewAdminService(&maintenanceSpy{repo: r}, scope)
	q := protocol.RecoveryRequest{Action: "record-fence", BindingID: "n-test", Operator: "owner", Nonce: testNonce}
	out, e := s.Do(ctx, AdminRequest{Scope: scope, Action: q.Action, Recovery: q})
	if e != nil {
		t.Fatal(e)
	}
	if out.Record == nil || out.Record.Kind != registry.KindFence || out.Record.Name != f.Name || out.Record.UID != f.UID {
		t.Fatal("admin returned invented operation rather than persisted fence")
	}
}

type failingAdmin struct{ err error }

func (f failingAdmin) Do(context.Context, AdminRequest) (AdminResponse, error) {
	return AdminResponse{}, f.err
}
func TestLocalAdminReturnsStructuredSafeFailure(t *testing.T) {
	scope := Scope{Context: "clo-lab", ProjectID: testProject, InstallationID: testInstall, Namespace: "clo-system"}
	input, _ := json.Marshal(AdminRequest{Scope: scope, Action: "inspect"})
	for _, check := range []struct {
		failure  error
		expected string
	}{
		{protocol.Fail(protocol.Blocked, "previous issuer termination unproven"), "previous issuer termination unproven"},
		{errors.New("private-credential"), "admin request rejected"},
	} {
		out := &bytes.Buffer{}
		if e := run(context.Background(), []string{"admin", "local"}, bytes.NewReader(input), out, Dependencies{LocalAdmin: failingAdmin{check.failure}}); e != nil {
			t.Fatal("failure lost in kubectl stderr", e)
		}
		var result struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(out.Bytes(), &result) != nil || result.Error != check.expected {
			t.Fatal("missing safe structured failure", out.String())
		}
	}
}
