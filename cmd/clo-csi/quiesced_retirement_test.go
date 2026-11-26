package main

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
)

func TestCLIQuiescedRetirementForwardsFullHandleAndRejectsUnknownInput(t *testing.T) {
	handle := "clo:" + testInstall + ":bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	args := []string{"admin", "retire-quiesced-format", "--context=clo-lab", "--project-id=" + testProject, "--installation-id=" + testInstall, "--volume=" + handle, "--operation=o-interrupted", "--operator=owner", "--nonce=" + testNonce, "--dry-run"}
	spy := &adminSpy{}
	if e := run(context.Background(), args, strings.NewReader(""), io.Discard, Dependencies{Admin: spy}); e != nil {
		t.Fatal(e)
	}
	if spy.calls != 1 || spy.requests[0].Action != "retire-quiesced-format" || spy.requests[0].Recovery.VolumeID != handle || spy.requests[0].Recovery.FenceID != "" || !spy.requests[0].Recovery.DryRun {
		t.Fatal("recovery scope or handle lost")
	}
	for _, invalid := range [][]string{append(append([]string{}, args...), "--force"), {"admin", "retire-quiesced-formats", "--context=clo-lab", "--project-id=" + testProject, "--installation-id=" + testInstall}} {
		if e := run(context.Background(), invalid, strings.NewReader(""), io.Discard, Dependencies{Admin: spy}); e == nil {
			t.Fatal("unknown input accepted")
		}
	}
	if spy.calls != 1 {
		t.Fatal("invalid input reached transport")
	}
}

func TestAdminQuiescedRetirementScopeAndDryRunMeaning(t *testing.T) {
	scope := Scope{Context: "clo-lab", ProjectID: testProject, InstallationID: testInstall, Namespace: "clo-system"}
	spy := &maintenanceSpy{repo: registry.New(registry.NewMemoryBackend(), registry.Installation{ID: testInstall, ProjectID: testProject})}
	service := NewAdminService(spy, scope)
	q := AdminRequest{Scope: scope, Action: "retire-quiesced-format", Recovery: protocol.RecoveryRequest{Action: "retire-quiesced-format", OperationID: "o-interrupted", VolumeID: "clo:" + testInstall + ":bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", Nonce: testNonce, Operator: "owner", DryRun: true}}
	for _, field := range []string{"project", "installation", "namespace", "action"} {
		bad := q
		switch field {
		case "project":
			bad.Scope.ProjectID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
		case "installation":
			bad.Scope.InstallationID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
		case "namespace":
			bad.Scope.Namespace = "other-system"
		case "action":
			bad.Recovery.Action = "reconcile"
		}
		if _, e := service.Do(context.Background(), bad); e == nil {
			t.Fatal("mismatched request accepted", field)
		}
	}
	if spy.calls != 0 {
		t.Fatal("scope mismatch reached coordinator")
	}
	out, e := service.Do(context.Background(), q)
	if e != nil {
		t.Fatal(e)
	}
	if spy.calls != 1 || spy.writes != 0 || spy.last.VolumeID != q.Recovery.VolumeID || out.Record == nil || out.Record.Name != "o-interrupted" {
		t.Fatal("dry run bypassed coordinator or lost operation")
	}
	if !strings.Contains(out.Message, "eligibility") || !strings.Contains(out.Message, "not established") {
		t.Fatal("dry run implies committed quiescence", out.Message)
	}
	q.Recovery.DryRun = false
	if _, e = service.Do(context.Background(), q); e != nil || spy.calls != 2 || spy.writes != 1 {
		t.Fatal("actual request not forwarded once", e)
	}
}

func TestAdminRejectsUnknownQuiescedRetirementFields(t *testing.T) {
	scope := Scope{Context: "clo-lab", ProjectID: testProject, InstallationID: testInstall, Namespace: "clo-system"}
	spy := &maintenanceSpy{repo: registry.New(registry.NewMemoryBackend(), registry.Installation{ID: testInstall, ProjectID: testProject})}
	req := httptest.NewRequest("POST", "/admin", bytes.NewBufferString(`{"scope":{"context":"clo-lab","projectID":"`+testProject+`","installationID":"`+testInstall+`","namespace":"clo-system"},"action":"retire-quiesced-format","force":true}`))
	out := httptest.NewRecorder()
	adminHandler(NewAdminService(spy, scope)).ServeHTTP(out, req)
	if out.Code != 400 || spy.calls != 0 {
		t.Fatal("unknown input reached coordinator", out.Code)
	}
}
