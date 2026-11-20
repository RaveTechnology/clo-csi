package coordinator

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RaveTechnology/clo-cloud-go/bootstrap"
	"github.com/RaveTechnology/clo-cloud-go/cloud"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"github.com/google/uuid"
)

type beforeClaimBackend struct {
	registry.Backend
	armed           atomic.Bool
	once            sync.Once
	match           func(registry.OperationSpec) bool
	entered, resume chan struct{}
}

func (b *beforeClaimBackend) Create(ctx context.Context, d registry.Document) (registry.Document, error) {
	if d.Kind == registry.KindOperation && b.armed.Load() {
		var spec registry.OperationSpec
		_ = json.Unmarshal(d.Spec, &spec)
		if b.match(spec) {
			b.once.Do(func() { close(b.entered); <-b.resume })
		}
	}
	return b.Backend.Create(ctx, d)
}
func claimBackend(match func(registry.OperationSpec) bool) *beforeClaimBackend {
	return &beforeClaimBackend{Backend: registry.NewMemoryBackend(), match: match, entered: make(chan struct{}), resume: make(chan struct{})}
}
func waitClaim(t *testing.T, b *beforeClaimBackend) {
	t.Helper()
	select {
	case <-b.entered:
	case <-time.After(time.Second):
		t.Fatal("request did not reach claim boundary")
	}
}

func TestConcurrentCompletedLargerExpandCannotBeFollowedByStaleShrink(t *testing.T) {
	b := claimBackend(func(s registry.OperationSpec) bool { return s.Type == "extend" && s.TargetSizeGB == 11 })
	h := setupBackend(t, true, b)
	v := published(t, h)
	handle := h.r.Handle(v.Status.ProviderID)
	b.armed.Store(true)
	type result struct {
		size int64
		err  error
	}
	done := make(chan result, 1)
	go func() { n, e := h.c.Expand(context.Background(), handle, 11<<30, 0); done <- result{n, e} }()
	waitClaim(t, b)
	if n, e := h.c.Expand(context.Background(), handle, 12<<30, 0); e != nil || n != 12<<30 {
		close(b.resume)
		t.Fatal("larger expansion failed", n, e)
	}
	close(b.resume)
	small := <-done
	if small.err != nil || small.size < 12<<30 || h.api.extendCalls != 1 {
		t.Fatalf("stale smaller request must observe current size without a lower POST: result=%+v native calls=%d", small, h.api.extendCalls)
	}
	actual, _ := h.api.GetVolume(context.Background(), v.Status.ProviderID)
	if actual.SizeGB != 12 {
		t.Fatal("native volume shrank", actual.SizeGB)
	}
}

type separateBaselineChannel struct {
	protocol.Channel
	second protocol.NodeRef
}

func (c separateBaselineChannel) Exchange(ctx context.Context, j protocol.Job) (protocol.Receipt, error) {
	if j.Node != c.second {
		return c.Channel.Exchange(ctx, j)
	}
	if j.Type != "baseline" {
		return protocol.Receipt{}, protocol.Fail(protocol.Unavailable, "second agent observe intentionally unavailable")
	}
	return protocol.Receipt{JobID: j.ID, Nonce: j.Nonce, Type: j.Type, Node: j.Node, Volume: j.Volume, ObservedAt: time.Now().UTC(), Snapshot: protocol.Snapshot{BootID: "boot-b", Disks: []protocol.Disk{{Path: "/dev/sda", Serial: "root-b", MajorMinor: "8:0", Root: true, Partitioned: true}}}}, nil
}

func TestPublishCannotClaimStaleUnattachedStateAfterOtherNodeCompletes(t *testing.T) {
	b := claimBackend(func(s registry.OperationSpec) bool { return s.Type == "attach" && s.NodeBindingID != "" })
	h := setupBackend(t, true, b)
	ctx := context.Background()
	secondID := uuid.NewString()
	h.api.extraServers = []cloud.Server{{ID: secondID, Name: "clo-second-attempt-b", ProjectID: projectID, Status: "ACTIVE", SwitchStatus: "ON", DiskIDs: []string{"boot-b"}}}
	h.api.volumes["boot-b"] = cloud.Volume{ID: "boot-b", Name: "boot-b", Status: "IN_USE", Bootable: true, SizeGB: 20, AttachedServerID: secondID}
	n, e := h.r.CreateNode(ctx, registry.NodeSpec{Ref: protocol.NodeRef{Name: "worker-b", UID: uuid.NewString(), ProjectID: projectID, ServerID: secondID, Epoch: 1}, Identity: bootstrap.MachineIdentity{SchemaVersion: 1, ProjectID: projectID, ProviderName: "clo-second-attempt-b", Attempt: "attempt-b"}, PrivateIP: "10.174.80.5"})
	if e != nil {
		t.Fatal(e)
	}
	n.Status.RootSerial = "root-b"
	n.Status.BootID = "boot-b"
	n.Status.BootVolumeIDs = []string{"boot-b"}
	n, e = h.r.SaveNode(ctx, n, n.Version)
	if e != nil {
		t.Fatal(e)
	}
	h.c.nodes = separateBaselineChannel{h.channel, n.Spec.Ref}
	b.match = func(s registry.OperationSpec) bool { return s.Type == "attach" && s.NodeBindingID == n.Name }
	v := create(t, h, "race-publish")
	handle := h.r.Handle(v.Status.ProviderID)
	b.armed.Store(true)
	done := make(chan error, 1)
	go func() { _, e := h.c.Publish(ctx, handle, EncodeNodeID(n.Spec.Ref), false); done <- e }()
	waitClaim(t, b)
	if _, e := h.c.Publish(ctx, handle, EncodeNodeID(h.node), false); e != nil {
		close(b.resume)
		t.Fatal("first node publish failed", e)
	}
	close(b.resume)
	_ = <-done
	if h.api.attachCalls != 1 {
		t.Fatalf("stale attachment crossed a completed claim and sent another POST: %d", h.api.attachCalls)
	}
	actual, _ := h.api.GetVolume(ctx, v.Status.ProviderID)
	if actual.AttachedServerID != serverID {
		t.Fatal("volume owner changed to stale request", actual.AttachedServerID)
	}
}
