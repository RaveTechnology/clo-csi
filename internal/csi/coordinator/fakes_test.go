package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/RaveTechnology/clo-cloud-go/bootstrap"
	"github.com/RaveTechnology/clo-cloud-go/cloud"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"github.com/google/uuid"
	"sync"
	"testing"
	"time"
)

const projectID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
const installationID = "11111111-1111-4111-8111-111111111111"
const nodeUID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
const serverID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
const controllerPod = "99999999-9999-4999-8999-999999999999"

type fakeCloud struct {
	cloud.API
	mu                                                                          sync.Mutex
	server                                                                      cloud.Server
	volumes                                                                     map[string]cloud.Volume
	extra                                                                       []cloud.Volume
	projectErr                                                                  error
	createErr                                                                   error
	createBeforeErr                                                             bool
	attachErr                                                                   error
	extendErr                                                                   error
	createCalls, attachCalls, detachCalls, extendCalls, deleteCalls, powerCalls int
	lastHot                                                                     bool
	attachHook                                                                  func()
	createSizeOverride, extendSizeOverride                                      int
	deleteDelay, pendingDeleteReads                                             int
	pendingDeleteID                                                             string
	extraServers                                                                []cloud.Server
}

func (f *fakeCloud) GetProject(context.Context, string) (cloud.Project, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloud.Project{ID: projectID, Status: "ACTIVE"}, f.projectErr
}
func (f *fakeCloud) GetServer(_ context.Context, id string) (cloud.Server, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var found *cloud.Server
	if f.server.ID != "" && f.server.ID == id {
		copy := f.server
		found = &copy
	}
	for _, s := range f.extraServers {
		if s.ID == id {
			copy := s
			found = &copy
		}
	}
	if found == nil {
		return cloud.Server{}, &cloud.APIError{Status: 404}
	}
	b, _ := json.Marshal(found)
	var s cloud.Server
	_ = json.Unmarshal(b, &s)
	return s, nil
}
func (f *fakeCloud) ListServers(ctx context.Context, _ string) ([]cloud.Server, error) {
	s, e := f.GetServer(ctx, serverID)
	f.mu.Lock()
	extras := append([]cloud.Server(nil), f.extraServers...)
	f.mu.Unlock()
	if cloud.IsNotFound(e) {
		return extras, nil
	}
	if e != nil {
		return nil, e
	}
	return append([]cloud.Server{s}, extras...), nil
}
func (f *fakeCloud) GetAddress(context.Context, string) (cloud.Address, error) {
	return cloud.Address{ID: "private", IP: "10.174.80.4", AttachedServerID: serverID}, nil
}
func (f *fakeCloud) GetVolume(_ context.Context, id string) (cloud.Volume, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.volumes[id]
	if !ok {
		return v, &cloud.APIError{Status: 404}
	}
	return v, nil
}
func (f *fakeCloud) ListVolumes(context.Context, string) ([]cloud.Volume, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pendingDeleteID != "" {
		if f.pendingDeleteReads == 0 {
			delete(f.volumes, f.pendingDeleteID)
			f.pendingDeleteID = ""
		} else {
			f.pendingDeleteReads--
		}
	}
	out := append([]cloud.Volume(nil), f.extra...)
	for _, v := range f.volumes {
		out = append(out, v)
	}
	return out, nil
}
func (f *fakeCloud) CreateVolume(_ context.Context, _ string, r cloud.CreateVolumeRequest) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	if f.createErr != nil && !f.createBeforeErr {
		return "", f.createErr
	}
	id := uuid.NewString()
	if f.createSizeOverride > 0 {
		r.SizeGB = f.createSizeOverride
	}
	f.volumes[id] = cloud.Volume{ID: id, Name: r.Name, Status: "AVAILABLE", SizeGB: r.SizeGB}
	return id, f.createErr
}
func (f *fakeCloud) AttachVolume(_ context.Context, id, server string) error {
	if f.attachHook != nil {
		f.attachHook()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attachCalls++
	if f.attachErr != nil {
		return f.attachErr
	}
	v := f.volumes[id]
	v.AttachedServerID = server
	v.Status = "IN_USE"
	v.Device = "/dev/WRONG-API-PATH"
	f.volumes[id] = v
	f.server.DiskIDs = append(f.server.DiskIDs, id)
	return nil
}
func (f *fakeCloud) DetachVolume(_ context.Context, id string, o cloud.DetachOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.detachCalls++
	f.lastHot = o.Hot
	v := f.volumes[id]
	v.AttachedServerID = ""
	v.Status = "AVAILABLE"
	f.volumes[id] = v
	var ids []string
	for _, x := range f.server.DiskIDs {
		if x != id {
			ids = append(ids, x)
		}
	}
	f.server.DiskIDs = ids
	return nil
}
func (f *fakeCloud) ExtendVolume(_ context.Context, id string, size int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.extendCalls++
	if f.extendErr != nil {
		return f.extendErr
	}
	v := f.volumes[id]
	v.SizeGB = size
	if f.extendSizeOverride > 0 {
		v.SizeGB = f.extendSizeOverride
	}
	f.volumes[id] = v
	return nil
}
func (f *fakeCloud) DeleteVolume(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls++
	if f.deleteDelay > 0 {
		f.pendingDeleteID = id
		f.pendingDeleteReads = f.deleteDelay
		return nil
	}
	delete(f.volumes, id)
	return nil
}
func (f *fakeCloud) Power(context.Context, string, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.powerCalls++
	return errors.New("forbidden power")
}
func (f *fakeCloud) DeleteServer(context.Context, string, []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.powerCalls++
	return errors.New("forbidden VM deletion")
}

type fakeChannel struct {
	mu           sync.Mutex
	cloud        *fakeCloud
	unavailable  bool
	staleBoot    bool
	extraDisks   int
	verdict      protocol.Verdict
	omitRoot     bool
	mounted      bool
	observeErr   bool
	locateCalls  int
	bootID       string
	descriptors  map[string]protocol.IssuerDescriptor
	lastLocated  protocol.IssuerDescriptor
	extraIssuers []protocol.IssuerDescriptor
}

func (n *fakeChannel) snapshot() protocol.Snapshot {
	n.cloud.mu.Lock()
	defer n.cloud.mu.Unlock()
	s := protocol.Snapshot{BootID: n.bootID}
	if !n.omitRoot {
		s.Disks = append(s.Disks, protocol.Disk{Path: "/dev/sda", Serial: "root-serial", MajorMinor: "8:0", Root: true, Partitioned: true})
	}
	for _, id := range n.cloud.server.DiskIDs {
		v := n.cloud.volumes[id]
		if v.Bootable {
			continue
		}
		s.Disks = append(s.Disks, protocol.Disk{Path: "/dev/sdz", Serial: "serial-" + id, MajorMinor: "8:16", SizeBytes: int64(v.SizeGB) << 30})
	}
	for i := 0; i < n.extraDisks; i++ {
		s.Disks = append(s.Disks, protocol.Disk{Path: "/dev/sdx", Serial: uuid.NewString(), MajorMinor: "8:32"})
	}
	return s
}
func (n *fakeChannel) Exchange(_ context.Context, j protocol.Job) (protocol.Receipt, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.unavailable {
		return protocol.Receipt{}, protocol.Fail(protocol.Unavailable, "node unreachable")
	}
	r := protocol.Receipt{JobID: j.ID, Nonce: j.Nonce, Type: j.Type, Node: j.Node, Volume: j.Volume, ObservedAt: time.Now().UTC()}
	switch j.Type {
	case "locateIssuer":
		n.locateCalls++
		if n.descriptors == nil {
			n.descriptors = map[string]protocol.IssuerDescriptor{}
		}
		descriptor, ok := n.descriptors[j.ProcessNonce]
		if !ok {
			descriptor = protocol.IssuerDescriptor{PodUID: j.PodUID, ProcessNonce: j.ProcessNonce, NodeUID: j.Node.UID, BindingID: j.Node.BindingID, ServerID: j.Node.ServerID, HostBootID: n.bootID, HostPID: 70 + len(n.descriptors), StartTicks: uint64(1000 + len(n.descriptors))}
			n.descriptors[j.ProcessNonce] = descriptor
		}
		r.Issuer = descriptor
		n.lastLocated = descriptor
	case "listIssuers":
		r.Issuers = append([]protocol.IssuerDescriptor{n.lastLocated}, n.extraIssuers...)
	case "verifyIssuer":
		r.Proof = protocol.IssuerProof{Descriptor: j.Issuer, Verdict: n.verdict, BootID: n.bootID}
	default:
		if n.observeErr && j.Type == "observe" {
			return r, errors.New("lost observation")
		}
		r.Snapshot = n.snapshot()
		if j.Type == "quiesce" {
			r.Quiesced = !n.mounted
			if n.staleBoot {
				r.Snapshot.BootID = "old-boot"
			}
		}
	}
	return r, nil
}

type harness struct {
	c       *Coordinator
	r       *registry.Repository
	api     *fakeCloud
	channel *fakeChannel
	node    protocol.NodeRef
	caller  protocol.Caller
	hello   protocol.NodeHello
	cfg     Config
}

func setup(t *testing.T, start bool) *harness {
	return setupBackend(t, start, registry.NewMemoryBackend())
}
func setupBackend(t *testing.T, start bool, backend registry.Backend) *harness {
	t.Helper()
	a := &fakeCloud{server: cloud.Server{ID: serverID, Name: "clo-worker-attempt", ProjectID: projectID, Status: "ACTIVE", SwitchStatus: "ON", AddressIDs: []string{"private"}, DiskIDs: []string{"boot-volume"}}, volumes: map[string]cloud.Volume{"boot-volume": {ID: "boot-volume", Name: "boot", Status: "IN_USE", Bootable: true, AttachedServerID: serverID}}}
	n := &fakeChannel{cloud: a, verdict: protocol.Alive, bootID: "boot-a"}
	r := registry.New(backend, registry.Installation{ID: installationID, ProjectID: projectID})
	cfg := Config{ProjectID: projectID, ControllerNodeUID: nodeUID, PollInterval: time.Millisecond, OperationTimeout: time.Second, MaxVolumesPerNode: 8}
	c := New(r, a, n, cfg)
	caller := protocol.Caller{NodeName: "worker-a", NodeUID: nodeUID, PodUID: uuid.NewString(), InternalIPs: []string{"10.174.80.4"}}
	hello := protocol.NodeHello{Identity: bootstrap.MachineIdentity{SchemaVersion: 1, ProjectID: projectID, ProviderName: a.server.Name, Attempt: "attempt"}, Snapshot: n.snapshot()}
	ref, e := c.RegisterNode(context.Background(), caller, hello)
	if e != nil {
		t.Fatal(e)
	}
	h := &harness{c, r, a, n, ref, caller, hello, cfg}
	if start {
		if e = c.StartIssuer(context.Background(), controllerPod, "first-process"); e != nil {
			t.Fatal(e)
		}
	}
	return h
}
func request(name string) protocol.VolumeRequest {
	return protocol.VolumeRequest{Name: name, RequiredBytes: 10 << 30, FSType: "ext4", AccessMode: "SINGLE_NODE_WRITER"}
}
func create(t *testing.T, h *harness, name string) registry.VolumeRecord {
	t.Helper()
	v, e := h.c.EnsureVolume(context.Background(), request(name))
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func published(t *testing.T, h *harness) registry.VolumeRecord {
	t.Helper()
	v := create(t, h, "pvc-a")
	if _, e := h.c.Publish(context.Background(), h.r.Handle(v.Status.ProviderID), EncodeNodeID(h.node), false); e != nil {
		t.Fatal(e)
	}
	v, e := h.r.Volume(context.Background(), v.Name)
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func mustBlocked(t *testing.T, e error) {
	t.Helper()
	if e == nil {
		t.Fatal("unsafe operation succeeded")
	}
}
