package main

import (
	"context"
	"encoding/json"
	"github.com/RaveTechnology/clo-csi/internal/csi/channel"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"io"
	"k8s.io/client-go/tools/clientcmd"
	api "k8s.io/client-go/tools/clientcmd/api"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRealBinaryBootstrapRPCWithoutCloudWrites(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "clo-csi")
	build := exec.Command("go", "build", "-trimpath", "-buildvcs=false", "-o", binary, ".")
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if output, e := build.CombinedOutput(); e != nil {
		t.Fatalf("build real binary: %s", output)
	}
	var proxyCalls atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { proxyCalls.Add(1); w.WriteHeader(502) }))
	defer proxy.Close()
	fakeAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/namespaces/clo-system/configmaps/clo-csi-installation":
			_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "clo-csi-installation", "namespace": "clo-system"}, "immutable": true, "data": map[string]string{"projectID": testProject, "installationID": testInstall}})
		case "/api/v1/namespaces/clo-system/pods/controller-test":
			_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": "controller-test", "namespace": "clo-system", "uid": testNonce}, "spec": map[string]any{"nodeName": "worker-test"}})
		case "/api/v1/nodes/worker-test":
			_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "Node", "metadata": map[string]any{"name": "worker-test", "uid": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "labels": map[string]string{"clo.csi.rave-technology.io/enabled": "true", "clo.csi.rave-technology.io/controller": "true"}}})
		case "/apis/storage.clo.rave-technology.io/v1alpha1/namespaces/clo-system/clonodebindings":
			_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "storage.clo.rave-technology.io/v1alpha1", "kind": "CloNodeBindingList", "metadata": map[string]string{"resourceVersion": "1"}, "items": []any{}})
		default:
			w.WriteHeader(404)
			_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "Status", "status": "Failure", "reason": "NotFound", "code": 404})
		}
	}))
	defer fakeAPI.Close()
	cfg := api.Config{Clusters: map[string]*api.Cluster{"fake": {Server: fakeAPI.URL}}, AuthInfos: map[string]*api.AuthInfo{"fake": {}}, Contexts: map[string]*api.Context{"clo-binary-test": {Cluster: "fake", AuthInfo: "fake", Namespace: "clo-system"}}, CurrentContext: "clo-binary-test"}
	raw, e := clientcmd.Write(cfg)
	if e != nil {
		t.Fatal(e)
	}
	configFile := filepath.Join(dir, "kubeconfig")
	_ = os.WriteFile(configFile, raw, 0600)
	material, e := channel.GenerateTLS([]string{"clo-csi-controller.clo-system.svc"}, nil, time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	cert, key, token := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key"), filepath.Join(dir, "token")
	_ = os.WriteFile(cert, material.CertPEM, 0600)
	_ = os.WriteFile(key, material.KeyPEM, 0600)
	_ = os.WriteFile(token, []byte("fake-test-credential"), 0600)
	socket := filepath.Join(dir, "csi.sock")
	args := []string{"controller", "--context=clo-binary-test", "--kubeconfig=" + configFile, "--project-id=" + testProject, "--installation-id=" + testInstall, "--namespace=clo-system", "--node-name=worker-test", "--pod-name=controller-test", "--pod-uid=" + testNonce, "--clo-token-file=" + token, "--tls-cert-file=" + cert, "--tls-key-file=" + key, "--endpoint=unix://" + socket, "--admin-socket=" + filepath.Join(dir, "admin.sock"), "--health-listen=127.0.0.1:0", "--channel-listen=127.0.0.1:0"}
	cmd := exec.Command(binary, args...)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	cmd.Env = append(os.Environ(), "HTTPS_PROXY="+proxy.URL, "HTTP_PROXY="+proxy.URL, "NO_PROXY=127.0.0.1,localhost")
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	connection, e := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))
	if e != nil {
		t.Fatal(e)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	identity := csi.NewIdentityClient(connection)
	info, e := identity.GetPluginInfo(ctx, &csi.GetPluginInfoRequest{})
	if e != nil || info.Name != "clo.csi.rave-technology.io" {
		t.Fatalf("real binary Identity RPC: %+v %v", info, e)
	}
	probe, e := identity.Probe(ctx, &csi.ProbeRequest{})
	if e != nil || probe.Ready == nil || probe.Ready.Value {
		t.Fatal("unregistered binary incorrectly ready", e)
	}
	controller := csi.NewControllerClient(connection)
	_, e = controller.CreateVolume(ctx, createSmokeRequest())
	if status.Code(e) != codes.Unavailable {
		t.Fatal("bootstrap controller dispatched mutation", e)
	}
	cmdline, e := os.ReadFile(filepath.Join("/proc", strconv.Itoa(cmd.Process.Pid), "cmdline"))
	if e != nil || !strings.Contains(string(cmdline), "--process-nonce=") {
		t.Fatal("nonce absent from real process argv", e)
	}
	if proxyCalls.Load() != 0 {
		t.Fatal("bootstrap-only smoke reached CLO network")
	}
}
func createSmokeRequest() *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{Name: "smoke", CapacityRange: &csi.CapacityRange{RequiredBytes: 10 << 30}, VolumeCapabilities: []*csi.VolumeCapability{{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}}}
}
