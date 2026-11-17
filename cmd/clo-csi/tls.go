package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/RaveTechnology/clo-csi/internal/csi/channel"
	"io"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"path/filepath"
	"sigs.k8s.io/yaml"
	"strings"
	"time"
)

const installationAnnotation = "storage.clo.rave-technology.io/installation-id"
const projectAnnotation = "storage.clo.rave-technology.io/project-id"

func tlsSecret(scope Scope, name string, m channel.TLSMaterial) corev1.Secret {
	return corev1.Secret{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"}, ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: scope.Namespace, Annotations: map[string]string{installationAnnotation: scope.InstallationID, projectAnnotation: scope.ProjectID}}, Type: corev1.SecretTypeTLS, Data: map[string][]byte{"tls.crt": m.CertPEM, "tls.key": m.KeyPEM, "ca.crt": m.CAPEM}}
}
func rotationSecrets(scope Scope, previous corev1.Secret, next channel.TLSMaterial) (corev1.Secret, corev1.Secret, error) {
	if previous.Namespace != scope.Namespace || previous.Name == "" || previous.Annotations[installationAnnotation] != scope.InstallationID || previous.Annotations[projectAnnotation] != scope.ProjectID {
		return corev1.Secret{}, corev1.Secret{}, errors.New("previous TLS secret scope differs")
	}
	if _, e := tls.X509KeyPair(previous.Data["tls.crt"], previous.Data["tls.key"]); e != nil {
		return corev1.Secret{}, corev1.Secret{}, errors.New("previous TLS material invalid")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(previous.Data["ca.crt"]) || !roots.AppendCertsFromPEM(next.CAPEM) {
		return corev1.Secret{}, corev1.Secret{}, errors.New("TLS rotation CA bundle invalid")
	}
	overlap := *previous.DeepCopy()
	overlap.Data["ca.crt"] = append(append([]byte(nil), previous.Data["ca.crt"]...), next.CAPEM...)
	active := tlsSecret(scope, previous.Name, next)
	active.Data["ca.crt"] = append([]byte(nil), overlap.Data["ca.crt"]...)
	return overlap, active, nil
}
func writePrivate(path string, data []byte) error {
	if path == "" {
		return errors.New("private output path required")
	}
	dir := filepath.Dir(path)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return errors.New("private output directory unavailable")
	}
	file, e := os.CreateTemp(dir, ".clo-private-")
	if e != nil {
		return errors.New("private output unavailable")
	}
	temp := file.Name()
	defer os.Remove(temp)
	defer file.Close()
	if e = file.Chmod(0600); e != nil {
		return e
	}
	if _, e = file.Write(data); e != nil {
		return e
	}
	if e = file.Sync(); e != nil {
		return e
	}
	if e = file.Close(); e != nil {
		return e
	}
	if e = os.Rename(temp, path); e != nil {
		return e
	}
	parent, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer parent.Close()
	return parent.Sync()
}
func writeTLSBootstrap(c Config, out io.Writer) error {
	if c.OutFile == "" {
		return errors.New("TLS bootstrap requires private --out file")
	}
	if c.DryRun {
		_, e := fmt.Fprintln(out, "Dry run: scoped TLS artifacts would be generated; no files written")
		return e
	}
	dns := []string{"clo-csi-controller." + c.Namespace + ".svc", "clo-csi-controller." + c.Namespace + ".svc.cluster.local"}
	if c.DNSNames != "" {
		dns = strings.Split(c.DNSNames, ",")
	}
	material, e := channel.GenerateTLS(dns, nil, 90*24*time.Hour)
	if e != nil {
		return errors.New("TLS bootstrap failed")
	}
	secret := tlsSecret(c.Scope, c.TLSSecret, material)
	if c.PreviousTLS != "" {
		raw, e := os.ReadFile(c.PreviousTLS)
		if e != nil {
			return errors.New("previous private TLS artifact unavailable")
		}
		var previous corev1.Secret
		if yaml.Unmarshal(raw, &previous) != nil {
			return errors.New("previous TLS manifest invalid")
		}
		overlap, activate, e := rotationSecrets(c.Scope, previous, material)
		if e != nil {
			return e
		}
		a, e := yaml.Marshal(overlap)
		if e != nil {
			return errors.New("TLS artifact encoding failed")
		}
		b, e := yaml.Marshal(activate)
		if e != nil {
			return errors.New("TLS artifact encoding failed")
		}
		if e = writePrivate(c.OutFile, a); e != nil {
			return e
		}
		if e = writePrivate(c.OutFile+".activate.yaml", b); e != nil {
			return e
		}
		_, e = fmt.Fprintln(out, "TLS overlap and activation artifacts written:", c.OutFile, c.OutFile+".activate.yaml")
		return e
	}
	raw, e := yaml.Marshal(secret)
	if e != nil {
		return errors.New("TLS artifact encoding failed")
	}
	if e = writePrivate(c.OutFile, raw); e != nil {
		return e
	}
	_, e = fmt.Fprintln(out, "TLS material written:", c.OutFile)
	return e
}
