package channel

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func tokenFile(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "token")
	if e := os.WriteFile(p, []byte("first-token"), 0600); e != nil {
		t.Fatal(e)
	}
	return p
}
func tlsServer(t *testing.T, m TLSMaterial) *httptest.Server {
	t.Helper()
	cert, e := tls.X509KeyPair(m.CertPEM, m.KeyPEM)
	if e != nil {
		t.Fatal(e)
	}
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(protocol.NodeRef{UID: nodeUID, BindingID: "binding"})
	}))
	s.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
	s.StartTLS()
	t.Cleanup(s.Close)
	return s
}
func TestCAOverlapAndStrictSAN(t *testing.T) {
	old, e := GenerateTLS([]string{"clo-csi-controller.clo-system.svc"}, []net.IP{net.ParseIP("127.0.0.1")}, time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	next, e := GenerateTLS([]string{"clo-csi-controller.clo-system.svc"}, []net.IP{net.ParseIP("127.0.0.1")}, time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	bundle := append(append([]byte(nil), old.CAPEM...), next.CAPEM...)
	p := tokenFile(t)
	for _, material := range []TLSMaterial{old, next} {
		s := tlsServer(t, material)
		c, e := NewClient(s.URL, bundle, p)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = c.Binding(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	wrong, e := GenerateTLS([]string{"another.example"}, nil, time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	s := tlsServer(t, wrong)
	c, e := NewClient(s.URL, wrong.CAPEM, p)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.Binding(context.Background()); e == nil {
		t.Fatal("wrong SAN accepted")
	}
	if _, e = NewClient(strings.Replace(s.URL, "https:", "http:", 1), bundle, p); e == nil {
		t.Fatal("plaintext channel accepted")
	}
}

type rejectAuth struct{}

func (rejectAuth) Authenticate(_ context.Context, token string) (protocol.Caller, error) {
	return protocol.Caller{}, fmt.Errorf("rejected %s", token)
}
func TestTokenAbsentFromLogsAndPublishContext(t *testing.T) {
	handler := NewServer(nil, rejectAuth{})
	req := httptest.NewRequest(http.MethodPost, "https://controller/register", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer do-not-echo-node-token")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	body, _ := io.ReadAll(w.Result().Body)
	if strings.Contains(string(body), "do-not-echo-node-token") {
		t.Fatal("token echoed in auth error")
	}
	p := tokenFile(t)
	var seen []string
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("Authorization"))
		_ = json.NewEncoder(w).Encode(protocol.NodeRef{})
	}))
	defer s.Close()
	pem := certificatePEM(s.Certificate())
	c, e := NewClient(s.URL, pem, p)
	if e != nil {
		t.Fatal(e)
	}
	_, _ = c.Binding(context.Background())
	_ = os.WriteFile(p, []byte("second-token"), 0600)
	_, _ = c.Binding(context.Background())
	if len(seen) != 2 || seen[0] != "Bearer first-token" || seen[1] != "Bearer second-token" {
		t.Fatal("projected token not read fresh")
	}
}
