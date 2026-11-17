package controller

import (
	"context"
	"errors"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
)

const installID = "11111111-1111-4111-8111-111111111111"
const providerID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
const nodeID = "clo-node:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa:cccccccc-cccc-4ccc-8ccc-cccccccccccc:bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
const handle = "clo:" + installID + ":" + providerID

type fakeService struct {
	ready             bool
	err               error
	calls             int
	publishedNode     string
	lastUnpublishNode string
	v                 registry.VolumeRecord
}

func fixtureService() *fakeService {
	return &fakeService{ready: true, v: registry.VolumeRecord{Meta: registry.Meta{Name: "volume-record"}, Spec: registry.VolumeSpec{InstallationID: installID, CSIName: "pvc-a", FSType: "ext4", AccessMode: "SINGLE_NODE_WRITER"}, Status: registry.VolumeStatus{ProviderID: providerID, SizeGB: 10, Serial: "stable-serial", FSUUID: providerID}}}
}
func (f *fakeService) Ready(context.Context) bool { return f.ready }
func (f *fakeService) EnsureVolume(_ context.Context, q protocol.VolumeRequest) (registry.VolumeRecord, error) {
	f.calls++
	if f.err != nil {
		return f.v, f.err
	}
	if q.RequiredBytes > 10<<30 {
		return f.v, protocol.Fail(protocol.Exists, "incompatible capacity")
	}
	return f.v, nil
}
func (f *fakeService) Delete(context.Context, string) error { f.calls++; return f.err }
func (f *fakeService) LookupVolume(context.Context, string) (registry.VolumeRecord, error) {
	f.calls++
	return f.v, f.err
}
func (f *fakeService) Publish(_ context.Context, _ string, node string, _ bool) (protocol.PublishContext, error) {
	f.calls++
	if f.err != nil {
		return protocol.PublishContext{}, f.err
	}
	if f.publishedNode != "" && f.publishedNode != node {
		return protocol.PublishContext{}, protocol.Fail(protocol.Blocked, "different owner")
	}
	f.publishedNode = node
	return protocol.PublishContext{Volume: protocol.VolumeRef{RecordID: f.v.Name, Handle: handle, ProviderID: providerID, Serial: f.v.Status.Serial, FSUUID: providerID, AttachEpoch: 1}, Node: protocol.NodeRef{Name: "worker-a", UID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", BindingID: "node-record", ServerID: "cccccccc-cccc-4ccc-8ccc-cccccccccccc", ProjectID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", Epoch: 1}}, nil
}
func (f *fakeService) Unpublish(_ context.Context, _ string, node string) error {
	f.calls++
	f.lastUnpublishNode = node
	return f.err
}
func (f *fakeService) Expand(context.Context, string, int64, int64) (int64, error) {
	f.calls++
	return 12 << 30, f.err
}
func rpc(t *testing.T, f *fakeService) (csi.ControllerClient, csi.IdentityClient) {
	t.Helper()
	l := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	srv := New(f, "test-version")
	csi.RegisterControllerServer(s, srv)
	csi.RegisterIdentityServer(s, srv)
	go s.Serve(l)
	t.Cleanup(s.Stop)
	cc, e := grpc.NewClient("passthrough:///bufconn", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return l.DialContext(ctx) }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { cc.Close() })
	return csi.NewControllerClient(cc), csi.NewIdentityClient(cc)
}
func capRWO() *csi.VolumeCapability {
	return &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}
}
func createRequest() *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{Name: "pvc-a", CapacityRange: &csi.CapacityRange{RequiredBytes: 10 << 30}, VolumeCapabilities: []*csi.VolumeCapability{capRWO()}}
}
func TestControllerRPCIdempotencyAndCodes(t *testing.T) {
	ctx := context.Background()
	f := fixtureService()
	c, _ := rpc(t, f)
	a, e := c.CreateVolume(ctx, createRequest())
	if e != nil || a.Volume.VolumeId != handle {
		t.Fatalf("create %+v %v", a, e)
	}
	b, e := c.CreateVolume(ctx, createRequest())
	if e != nil || b.Volume.VolumeId != a.Volume.VolumeId {
		t.Fatal("repeat create changed handle")
	}
	q := createRequest()
	q.CapacityRange.RequiredBytes = 11 << 30
	_, e = c.CreateVolume(ctx, q)
	if status.Code(e) != codes.AlreadyExists {
		t.Fatal(e)
	}
	for _, tt := range []struct {
		err  error
		code codes.Code
	}{{protocol.Fail(protocol.Conflict, "pending"), codes.Aborted}, {protocol.Fail(protocol.Blocked, "owner"), codes.FailedPrecondition}, {protocol.Fail(protocol.RecoveryRequired, "uncertain"), codes.FailedPrecondition}, {protocol.Fail(protocol.Unavailable, "transient"), codes.Unavailable}, {errors.New("do-not-echo-secret"), codes.Internal}} {
		f.err = tt.err
		_, e = c.CreateVolume(ctx, createRequest())
		if status.Code(e) != tt.code || strings.Contains(e.Error(), "do-not-echo-secret") {
			t.Fatalf("mapped %v to %v", tt.err, e)
		}
	}
}
func TestControllerOptionalNodeIDAndReadonly(t *testing.T) {
	ctx := context.Background()
	f := fixtureService()
	c, _ := rpc(t, f)
	q := &csi.ControllerPublishVolumeRequest{VolumeId: handle, NodeId: nodeID, VolumeCapability: capRWO()}
	a, e := c.ControllerPublishVolume(ctx, q)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.ControllerPublishVolume(ctx, q); e != nil {
		t.Fatal(e)
	}
	for k, v := range a.PublishContext {
		if strings.Contains(strings.ToLower(k), "token") || strings.Contains(v, "secret") {
			t.Fatal("secret in publish context")
		}
	}
	other := proto.Clone(q).(*csi.ControllerPublishVolumeRequest)
	other.NodeId = strings.Replace(nodeID, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "ffffffff-ffff-4fff-8fff-ffffffffffff", 1)
	_, e = c.ControllerPublishVolume(ctx, other)
	if status.Code(e) != codes.FailedPrecondition {
		t.Fatal(e)
	}
	q.Readonly = true
	before := f.calls
	_, e = c.ControllerPublishVolume(ctx, q)
	if status.Code(e) != codes.InvalidArgument || f.calls != before {
		t.Fatal("controller readonly reached service")
	}
	if _, e = c.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: handle}); e != nil || f.lastUnpublishNode != "" {
		t.Fatal("optional node ID rejected", e)
	}
}
func TestUnsupportedCapabilitiesAreNotAdvertised(t *testing.T) {
	f := fixtureService()
	c, i := rpc(t, f)
	caps, e := c.ControllerGetCapabilities(context.Background(), &csi.ControllerGetCapabilitiesRequest{})
	if e != nil {
		t.Fatal(e)
	}
	want := map[csi.ControllerServiceCapability_RPC_Type]bool{csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME: true, csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME: true, csi.ControllerServiceCapability_RPC_EXPAND_VOLUME: true}
	for _, v := range caps.Capabilities {
		if !want[v.GetRpc().Type] {
			t.Fatal("unsupported capability", v)
		}
	}
	if len(caps.Capabilities) != 3 {
		t.Fatal("missing caps")
	}
	_, e = c.GetCapacity(context.Background(), &csi.GetCapacityRequest{})
	if status.Code(e) != codes.Unimplemented {
		t.Fatal(e)
	}
	q := createRequest()
	q.VolumeCapabilities[0].AccessMode.Mode = csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER
	before := f.calls
	_, e = c.CreateVolume(context.Background(), q)
	if status.Code(e) != codes.InvalidArgument || before != f.calls {
		t.Fatal("RWX reached service")
	}
	plugin, e := i.GetPluginInfo(context.Background(), &csi.GetPluginInfoRequest{})
	if e != nil || plugin.Name != protocol.DriverName {
		t.Fatal("wrong driver identity")
	}
}
func TestControllerBootstrapBeforeReadiness(t *testing.T) {
	f := fixtureService()
	f.ready = false
	_, i := rpc(t, f)
	p, e := i.Probe(context.Background(), &csi.ProbeRequest{})
	if e != nil || p.Ready == nil || p.Ready.Value {
		t.Fatal("Probe must report false without killing server")
	}
	h := Health(f)
	for _, tt := range []struct {
		path string
		code int
	}{{"/livez", 200}, {"/readyz", 503}} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", tt.path, nil))
		if w.Code != tt.code {
			t.Fatalf("%s=%d", tt.path, w.Code)
		}
	}
}
func TestInvalidHandleDoesNotReachCloud(t *testing.T) {
	f := fixtureService()
	c, _ := rpc(t, f)
	for _, tt := range []struct {
		id   string
		want codes.Code
	}{
		{"", codes.InvalidArgument}, {"random", codes.OK}, {"clo:wrong:wrong", codes.OK}, {"clo:" + installID + ":../../dev/sda", codes.OK},
	} {
		before := f.calls
		_, e := c.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{VolumeId: tt.id})
		if status.Code(e) != tt.want || f.calls != before {
			t.Fatalf("non-owned handle must be an idempotent no-op without reaching service: %q %v", tt.id, e)
		}
	}
}
