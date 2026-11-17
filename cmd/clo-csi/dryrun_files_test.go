package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
)

func TestExportDryRunThroughAdminServicePreservesExistingBackup(t *testing.T) {
	ctx := context.Background()
	r := registry.New(registry.NewMemoryBackend(), registry.Installation{ID: testInstall, ProjectID: testProject})
	service := NewAdminService(&maintenanceSpy{repo: r}, Scope{Context: "clo-lab", ProjectID: testProject, InstallationID: testInstall, Namespace: "clo-system"})
	file := filepath.Join(t.TempDir(), "registry.json")
	original := []byte("irreplaceable backup bytes")
	if e := os.WriteFile(file, original, 0600); e != nil {
		t.Fatal(e)
	}
	out := &bytes.Buffer{}
	args := append(argsFor("export"), "--out="+file, "--dry-run")
	if e := run(ctx, args, bytes.NewReader(nil), out, Dependencies{Admin: service}); e != nil {
		t.Fatal(e)
	}
	got, e := os.ReadFile(file)
	if e != nil || !bytes.Equal(got, original) || strings.Contains(out.String(), "backup written") {
		t.Fatalf("dry run changed backup or claimed write: %q %s %v", got, out, e)
	}
}

func TestTLSBootstrapDryRunDoesNotCreateOrReplaceFiles(t *testing.T) {
	for _, existing := range []bool{false, true} {
		dir := t.TempDir()
		file := filepath.Join(dir, "tls.yaml")
		original := []byte("existing private TLS")
		if existing {
			if e := os.WriteFile(file, original, 0600); e != nil {
				t.Fatal(e)
			}
		}
		out := &bytes.Buffer{}
		args := append(argsFor("tls-bootstrap"), "--out="+file, "--dry-run")
		if e := run(context.Background(), args, bytes.NewReader(nil), out, Dependencies{}); e != nil {
			t.Fatal(e)
		}
		got, e := os.ReadFile(file)
		if existing {
			if e != nil || !bytes.Equal(got, original) {
				t.Fatal("TLS dry run replaced existing material")
			}
		} else if !os.IsNotExist(e) {
			t.Fatal("TLS dry run created material")
		}
		if _, e := os.Stat(strings.TrimSuffix(file, ".yaml") + ".activate.yaml"); !os.IsNotExist(e) {
			t.Fatal("TLS dry run created activation file")
		}
	}
}

type backupResponseClient struct{ data []byte }

func (s backupResponseClient) Do(context.Context, AdminRequest) (AdminResponse, error) {
	return AdminResponse{Data: s.data}, nil
}

func TestExportRefusesInvalidResponseBeforeReplacingBackup(t *testing.T) {
	for _, data := range [][]byte{nil, []byte("not a backup"), []byte(`{"data":{},"sha256":"wrong"}`)} {
		file := filepath.Join(t.TempDir(), "backup.json")
		original := []byte("previous backup")
		if e := os.WriteFile(file, original, 0600); e != nil {
			t.Fatal(e)
		}
		out := &bytes.Buffer{}
		e := run(context.Background(), append(argsFor("export"), "--out="+file), bytes.NewReader(nil), out, Dependencies{Admin: backupResponseClient{data}})
		got, _ := os.ReadFile(file)
		if e == nil || !bytes.Equal(got, original) || strings.Contains(out.String(), "Registry backup written") {
			t.Fatal("invalid response destroyed backup", e)
		}
	}
}

func TestExportWritesValidatedScopedBackup(t *testing.T) {
	ctx := context.Background()
	r := registry.New(registry.NewMemoryBackend(), registry.Installation{ID: testInstall, ProjectID: testProject})
	if e := r.SetFrozen(ctx, true); e != nil {
		t.Fatal(e)
	}
	data, e := r.Export(ctx)
	if e != nil {
		t.Fatal(e)
	}
	file := filepath.Join(t.TempDir(), "backup.json")
	out := &bytes.Buffer{}
	if e = run(ctx, append(argsFor("export"), "--out="+file), bytes.NewReader(nil), out, Dependencies{Admin: backupResponseClient{data}}); e != nil {
		t.Fatal(e)
	}
	got, e := os.ReadFile(file)
	if e != nil || !bytes.Equal(got, data) {
		t.Fatal("valid backup not written", e)
	}
}
