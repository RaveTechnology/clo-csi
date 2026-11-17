package node

import (
	"context"
	"encoding/json"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"math"
	"net"
	"testing"
)

type nodeAuth struct {
	p                  protocol.StagePermit
	lost, formatIssued bool
	reports            int
}

func (a *nodeAuth) Register(context.Context, protocol.NodeHello) (protocol.NodeRef, error) {
	return a.p.Node, nil
}
func (a *nodeAuth) Binding(context.Context) (protocol.NodeRef, error) { return a.p.Node, nil }
func (a *nodeAuth) AuthorizeStage(_ context.Context, q protocol.StageRequest) (protocol.StagePermit, error) {
	if a.lost {
		return protocol.StagePermit{}, protocol.Fail(protocol.RecoveryRequired, "registry unavailable")
	}
	p := a.p
	p.AllowFormat = false
	if q.Format {
		if a.formatIssued {
			return p, protocol.Fail(protocol.RecoveryRequired, "permit consumed")
		}
		a.formatIssued = true
		a.p.Initialization = "FormatDispatched"
		p.Initialization = "FormatDispatched"
		p.AllowFormat = true
	}
	return p, nil
}
func (a *nodeAuth) Report(_ context.Context, r protocol.Receipt) error {
	if !r.FilesystemChecked || r.Type != "formatComplete" || r.Volume != a.p.Volume {
		return protocol.Fail(protocol.Blocked, "format receipt differs")
	}
	a.reports++
	a.p.Initialization = "FormatComplete"
	return nil
}
func nodeCap() *csi.VolumeCapability {
	return &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}
}
func nodeRPC(t *testing.T, h *fsHarness, a *nodeAuth) (csi.NodeClient, csi.IdentityClient) {
	t.Helper()
	l := bufconn.Listen(1 << 20)
	g := grpc.NewServer()
	s := NewServer(h.r, a, NodeConfig{KubeletRoot: h.root, Version: "test", MaxVolumesPerNode: 8})
	csi.RegisterNodeServer(g, s)
	csi.RegisterIdentityServer(g, s)
	go g.Serve(l)
	t.Cleanup(g.Stop)
	cc, e := grpc.NewClient("passthrough:///node", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return l.DialContext(ctx) }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { cc.Close() })
	return csi.NewNodeClient(cc), csi.NewIdentityClient(cc)
}
func stageRequest(h *fsHarness) *csi.NodeStageVolumeRequest {
	raw, _ := json.Marshal(protocol.PublishContext{Volume: h.p.Volume, Node: h.p.Node})
	return &csi.NodeStageVolumeRequest{VolumeId: fsHandle, PublishContext: map[string]string{"clo-context": string(raw)}, StagingTargetPath: h.stage, VolumeCapability: nodeCap()}
}
func TestLostRegistryDeniesNewFormat(t *testing.T) {
	h := newFS(t, false)
	a := &nodeAuth{p: h.p, lost: true}
	c, _ := nodeRPC(t, h, a)
	_, e := c.NodeStageVolume(context.Background(), stageRequest(h))
	if status.Code(e) != codes.FailedPrecondition || h.fs.formats != 0 || h.fs.mounts != 0 {
		t.Fatal("lost registry permitted format", e)
	}
}
func TestNodeRPCFilesystemWorkflow(t *testing.T) {
	h := newFS(t, false)
	a := &nodeAuth{p: h.p}
	a.p.AllowFormat = false
	ctx := context.Background()
	c, i := nodeRPC(t, h, a)
	q := stageRequest(h)
	if _, e := c.NodeStageVolume(ctx, q); e != nil {
		t.Fatal(e)
	}
	if _, e := c.NodeStageVolume(ctx, q); e != nil {
		t.Fatal(e)
	}
	if h.fs.formats != 1 || a.reports != 1 {
		t.Fatal("RPC format/report repeated")
	}
	pub := &csi.NodePublishVolumeRequest{VolumeId: fsHandle, PublishContext: q.PublishContext, StagingTargetPath: h.stage, TargetPath: h.targetA, VolumeCapability: nodeCap()}
	if _, e := c.NodePublishVolume(ctx, pub); e != nil {
		t.Fatal(e)
	}
	other := proto.Clone(pub).(*csi.NodePublishVolumeRequest)
	other.TargetPath = h.targetB
	other.Readonly = true
	if _, e := c.NodePublishVolume(ctx, other); e != nil {
		t.Fatal(e)
	}
	stats, e := c.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{VolumeId: fsHandle, VolumePath: h.stage})
	if e != nil || len(stats.Usage) != 2 {
		t.Fatal("stats", e)
	}
	size, e := c.NodeExpandVolume(ctx, &csi.NodeExpandVolumeRequest{VolumeId: fsHandle, VolumePath: h.stage, CapacityRange: &csi.CapacityRange{RequiredBytes: 12 << 30}, VolumeCapability: nodeCap()})
	if e != nil || size.CapacityBytes < 12<<30 {
		t.Fatal("expand", e)
	}
	_, e = c.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: fsHandle, StagingTargetPath: h.stage})
	if status.Code(e) != codes.FailedPrecondition {
		t.Fatal("unstage active refs", e)
	}
	for _, target := range []string{h.targetA, h.targetB} {
		if _, e = c.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: fsHandle, TargetPath: target}); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = c.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: fsHandle, StagingTargetPath: h.stage}); e != nil {
		t.Fatal(e)
	}
	info, e := c.NodeGetInfo(ctx, &csi.NodeGetInfoRequest{})
	if e != nil || info.MaxVolumesPerNode != 8 || info.NodeId != "clo-node:"+h.p.Node.ProjectID+":"+h.p.Node.ServerID+":"+h.p.Node.UID {
		t.Fatal("node identity", e)
	}
	caps, e := c.NodeGetCapabilities(ctx, &csi.NodeGetCapabilitiesRequest{})
	if e != nil || len(caps.Capabilities) != 3 {
		t.Fatal("node caps", e)
	}
	plugin, e := i.GetPluginInfo(ctx, &csi.GetPluginInfoRequest{})
	if e != nil || plugin.Name != protocol.DriverName {
		t.Fatal("driver identity", e)
	}
}
func TestVerifyFormatJobChecksUnmountedOwnedFilesystem(t *testing.T) {
	h := newFS(t, true)
	r := &Runtime{Inspector: h.ins, Journal: h.j, Filesystem: h.fs, Node: h.r.Node}
	job := protocol.Job{ID: "verify", Nonce: "nonce", Type: "verifyFormat", Node: h.p.Node, Volume: h.p.Volume}
	receipt, e := r.Execute(context.Background(), job)
	if e != nil || !receipt.FilesystemChecked || h.fs.checks != 1 || h.fs.formats != 0 {
		t.Fatal("read-only format recovery proof", e)
	}
}

func TestNodeExpansionOverflowRejectedBeforeFilesystemMutation(t *testing.T) {
	h := newFS(t, true)
	h.stageReady(t)
	c, _ := nodeRPC(t, h, &nodeAuth{p: h.p})
	_, e := c.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{VolumeId: fsHandle, VolumePath: h.stage, CapacityRange: &csi.CapacityRange{RequiredBytes: math.MaxInt64}, VolumeCapability: nodeCap()})
	if status.Code(e) != codes.InvalidArgument || h.fs.expands != 0 {
		t.Fatal("invalid expansion reached filesystem", e)
	}
}
