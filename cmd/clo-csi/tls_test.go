package main

import (
	"bytes"
	"context"
	"encoding/pem"
	"github.com/RaveTechnology/clo-csi/internal/csi/channel"
	corev1 "k8s.io/api/core/v1"
	"os"
	"path/filepath"
	"sigs.k8s.io/yaml"
	"testing"
	"time"
)

func TestTLSBootstrapAndRotation(t *testing.T) {
	scope := Scope{Context: "clo-lab", ProjectID: testProject, InstallationID: testInstall, Namespace: "clo-system"}
	old, e := channel.GenerateTLS([]string{"clo-csi-controller.clo-system.svc"}, nil, time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	secret := tlsSecret(scope, "clo-csi-tls", old)
	next, e := channel.GenerateTLS([]string{"clo-csi-controller.clo-system.svc"}, nil, time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	overlap, active, e := rotationSecrets(scope, secret, next)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(overlap.Data["tls.crt"], old.CertPEM) || !bytes.Equal(overlap.Data["tls.key"], old.KeyPEM) {
		t.Fatal("CA overlap prematurely replaced serving keypair")
	}
	if bytes.Equal(active.Data["tls.crt"], old.CertPEM) || !bytes.Equal(active.Data["tls.crt"], next.CertPEM) {
		t.Fatal("rotation did not activate next certificate")
	}
	rest := active.Data["ca.crt"]
	count := 0
	for {
		block, left := pem.Decode(rest)
		if block == nil {
			break
		}
		count++
		rest = left
	}
	if count != 2 {
		t.Fatal("CA overlap absent")
	}
	secret.Namespace = "foreign"
	if _, _, e = rotationSecrets(scope, secret, next); e == nil {
		t.Fatal("cross-scope TLS secret accepted")
	}
	dir := t.TempDir()
	out := &bytes.Buffer{}
	args := []string{"admin", "tls-bootstrap", "--context=clo-lab", "--project-id=" + testProject, "--installation-id=" + testInstall, "--namespace=clo-system", "--out=" + filepath.Join(dir, "tls.yaml")}
	if e = run(context.Background(), args, bytes.NewReader(nil), out, Dependencies{}); e != nil {
		t.Fatal(e)
	}
	data, e := os.ReadFile(filepath.Join(dir, "tls.yaml"))
	if e != nil {
		t.Fatal(e)
	}
	if bytes.Contains(out.Bytes(), []byte("PRIVATE KEY")) || bytes.Contains(out.Bytes(), []byte("tls.key")) {
		t.Fatal("private TLS material printed")
	}
	stat, e := os.Stat(filepath.Join(dir, "tls.yaml"))
	if e != nil || stat.Mode().Perm() != 0600 {
		t.Fatal("TLS artifact permissions")
	}
	var decoded corev1.Secret
	if e = yaml.Unmarshal(data, &decoded); e != nil || decoded.Kind != "Secret" {
		t.Fatal("TLS manifest not a Secret", e)
	}
}
