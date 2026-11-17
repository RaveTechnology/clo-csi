package integration

import (
	"context"
	"errors"
	"fmt"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	"github.com/kubernetes-csi/csi-test/v5/pkg/sanity"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const labProject = "11111111-2222-4333-8444-555555555555"
const labInstallation = "11111111-1111-4111-8111-111111111111"
const labPod = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"

func TestCSISanity(t *testing.T) {
	if os.Getenv("CLO_CSI_SANITY_LAB") == "" {
		t.Skip("live CLO lab is opt-in; default tests never create cloud resources")
	}
	config, e := labConfiguration(os.Getenv)
	if e != nil {
		t.Fatal(e)
	}
	for _, socket := range []string{config.Address, config.ControllerAddress} {
		info, e := os.Lstat(strings.TrimPrefix(socket, "unix://"))
		if e != nil || info.Mode()&os.ModeSocket == 0 {
			t.Fatal("explicit lab CSI socket missing", socket, e)
		}
	}
	sanity.Test(t, config)
}

type idGenerator struct{ installation string }

func (g idGenerator) GenerateUniqueValidVolumeID() string {
	return "clo:" + g.installation + ":" + uuid.NewString()
}
func (g idGenerator) GenerateInvalidVolumeID() string { return "invalid-volume" }
func (g idGenerator) GenerateUniqueValidNodeID() string {
	return "clo-node:" + labProject + ":" + uuid.NewString() + ":" + uuid.NewString()
}
func (g idGenerator) GenerateInvalidNodeID() string { return "invalid-node" }

func targetPrefix(installation, podUID string) string {
	return "/var/lib/kubelet/pods/" + podUID + "/volumes/kubernetes.io~csi/clo-sanity-" + installation + "/"
}

// Upstream sanity appends literal /target and /other_target. Adapt exactly these
// test-owned aliases into real kubelet mount paths; leave negative inputs intact.
func ownedPath(installation, podUID, path string) string {
	prefix := targetPrefix(installation, podUID)
	if filepath.Clean(path) != path || !strings.HasPrefix(path, prefix) {
		return path
	}
	parts := strings.Split(strings.TrimPrefix(path, prefix), "/")
	if len(parts) != 2 || (parts[1] != "target" && parts[1] != "other_target") {
		return path
	}
	if id, e := uuid.Parse(parts[0]); e != nil || id.String() != parts[0] {
		return path
	}
	return path + "/mount"
}
func pathAdapter(installation, podUID string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, request, response any, conn *grpc.ClientConn, invoke grpc.UnaryInvoker, options ...grpc.CallOption) error {
		switch q := request.(type) {
		case *csi.NodePublishVolumeRequest:
			q = proto.Clone(q).(*csi.NodePublishVolumeRequest)
			q.TargetPath = ownedPath(installation, podUID, q.TargetPath)
			request = q
		case *csi.NodeUnpublishVolumeRequest:
			q = proto.Clone(q).(*csi.NodeUnpublishVolumeRequest)
			q.TargetPath = ownedPath(installation, podUID, q.TargetPath)
			request = q
		case *csi.NodeGetVolumeStatsRequest:
			q = proto.Clone(q).(*csi.NodeGetVolumeStatsRequest)
			q.VolumePath = ownedPath(installation, podUID, q.VolumePath)
			request = q
		case *csi.NodeExpandVolumeRequest:
			q = proto.Clone(q).(*csi.NodeExpandVolumeRequest)
			q.VolumePath = ownedPath(installation, podUID, q.VolumePath)
			request = q
		}
		return invoke(ctx, method, request, response, conn, options...)
	}
}
func removeEmpty(path string) error {
	e := os.Remove(path)
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	return e
}
func labConfiguration(getenv func(string) string) (sanity.TestConfig, error) {
	cfg := sanity.NewTestConfig()
	installation := getenv("CLO_CSI_SANITY_INSTALLATION_ID")
	id, e := uuid.Parse(installation)
	if getenv("CLO_CSI_SANITY_LAB") != "1" || getenv("CLO_CSI_SANITY_PROJECT_ID") != labProject || e != nil || id.String() != installation {
		return cfg, fmt.Errorf("explicit owned CLO lab project/installation required")
	}
	podUID := getenv("CLO_CSI_SANITY_POD_UID")
	if id, e := uuid.Parse(podUID); e != nil || id.String() != podUID {
		return cfg, fmt.Errorf("explicit actual sanity-runner Pod UID required")
	}
	cfg.Address = getenv("CLO_CSI_SANITY_NODE")
	cfg.ControllerAddress = getenv("CLO_CSI_SANITY_CONTROLLER")
	if cfg.Address != "unix:///var/lib/kubelet/plugins/clo.csi.rave-technology.io/csi.sock" || cfg.ControllerAddress != "unix:///controller/csi.sock" {
		return cfg, fmt.Errorf("explicit separate lab node/controller Unix sockets required")
	}
	cfg.TestVolumeSize = 10 << 30
	cfg.TestVolumeExpandSize = 12 << 30
	cfg.IDGen = idGenerator{installation}
	cfg.DialOptions = append(cfg.DialOptions, grpc.WithChainUnaryInterceptor(pathAdapter(installation, podUID)))
	cfg.TargetPath = targetPrefix(installation, podUID)
	cfg.StagingPath = "/var/lib/kubelet/plugins/kubernetes.io/csi/clo.csi.rave-technology.io/sanity-" + installation
	cfg.CreateTargetDir = func(string) (string, error) {
		p := targetPrefix(installation, podUID) + uuid.NewString()
		return p, os.MkdirAll(p, 0750)
	}
	cfg.CreateStagingDir = func(string) (string, error) {
		p := cfg.StagingPath + "/" + uuid.NewString() + "/globalmount"
		return p, os.MkdirAll(filepath.Dir(p), 0750)
	}
	cfg.RemoveTargetPath = func(p string) error {
		for _, alias := range []string{"target", "other_target"} {
			if e := removeEmpty(filepath.Join(p, alias)); e != nil {
				return e
			}
		}
		return removeEmpty(p)
	}
	cfg.RemoveStagingPath = func(p string) error {
		if e := removeEmpty(p); e != nil {
			return e
		}
		return removeEmpty(filepath.Dir(p))
	}
	cfg.CheckPath = func(p string) (sanity.PathKind, error) {
		info, e := os.Stat(ownedPath(installation, podUID, p))
		if errors.Is(e, os.ErrNotExist) {
			return sanity.PathIsNotFound, nil
		}
		if e != nil {
			return "", e
		}
		if info.IsDir() {
			return sanity.PathIsDir, nil
		}
		if info.Mode().IsRegular() {
			return sanity.PathIsFile, nil
		}
		return sanity.PathIsOther, nil
	}
	return cfg, nil
}

func validEnvironment() map[string]string {
	return map[string]string{
		"CLO_CSI_SANITY_LAB": "1", "CLO_CSI_SANITY_PROJECT_ID": labProject,
		"CLO_CSI_SANITY_INSTALLATION_ID": labInstallation,
		"CLO_CSI_SANITY_POD_UID":         labPod,
		"CLO_CSI_SANITY_NODE":            "unix:///var/lib/kubelet/plugins/clo.csi.rave-technology.io/csi.sock",
		"CLO_CSI_SANITY_CONTROLLER":      "unix:///controller/csi.sock",
	}
}

func TestLabConfigRejectsImplicitEndpointsAndScope(t *testing.T) {
	for _, key := range []string{"CLO_CSI_SANITY_LAB", "CLO_CSI_SANITY_PROJECT_ID", "CLO_CSI_SANITY_INSTALLATION_ID", "CLO_CSI_SANITY_POD_UID", "CLO_CSI_SANITY_NODE", "CLO_CSI_SANITY_CONTROLLER"} {
		env := validEnvironment()
		delete(env, key)
		if _, e := labConfiguration(func(k string) string { return env[k] }); e == nil {
			t.Fatal("implicit lab input accepted", key)
		}
	}
	env := validEnvironment()
	cfg, e := labConfiguration(func(k string) string { return env[k] })
	if e != nil || cfg.Address == cfg.ControllerAddress || cfg.TestVolumeSize != 10<<30 || cfg.TestVolumeExpandSize != 12<<30 || !strings.Contains(cfg.TargetPath, "/pods/"+labPod+"/") {
		t.Fatal("wrong separated/10Gi lab configuration", e)
	}
	env["CLO_CSI_SANITY_PROJECT_ID"] = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	if _, e = labConfiguration(func(k string) string { return env[k] }); e == nil {
		t.Fatal("foreign project accepted")
	}
}

func TestSanityPathAdapterOnlyMapsOwnedAliases(t *testing.T) {
	root := "/var/lib/kubelet/pods/" + labPod + "/volumes/kubernetes.io~csi/clo-sanity-" + labInstallation + "/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	adapt := pathAdapter(labInstallation, labPod)
	for _, path := range []string{root + "/target", root + "/other_target", "", root + "/../target", "/outside/target", strings.Replace(root, labInstallation, "other-install", 1) + "/target", strings.Replace(root, labPod, "ffffffff-ffff-4fff-8fff-ffffffffffff", 1) + "/target"} {
		req := &csi.NodePublishVolumeRequest{TargetPath: path}
		expected := path
		if path == root+"/target" || path == root+"/other_target" {
			expected += "/mount"
		}
		called := false
		e := adapt(context.Background(), "/csi.v1.Node/NodePublishVolume", req, &csi.NodePublishVolumeResponse{}, nil, func(_ context.Context, _ string, q, r any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
			called = true
			if q.(*csi.NodePublishVolumeRequest).TargetPath != expected {
				t.Fatal("unexpected path translation", q)
			}
			return nil
		})
		if e != nil || !called || req.TargetPath != path {
			t.Fatal("adapter changed caller request or swallowed RPC", e)
		}
	}
}
