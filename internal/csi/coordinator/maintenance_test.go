package coordinator

import (
	"context"
	"encoding/json"
	"github.com/RaveTechnology/clo-cloud-go/cloud"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"github.com/google/uuid"
	"sync"
	"testing"
	"time"
)

func TestMaintenanceRequestsAreAuditedAndSurviveRestore(t *testing.T) {
	h := setup(t, true)
	ctx := context.Background()
	q := protocol.RecoveryRequest{Action: "freeze", Operator: "owner", Nonce: uuid.NewString()}
	q.DryRun = true
	if _, e := h.c.Maintain(ctx, q, nil); e != nil {
		t.Fatal(e)
	}
	i, _ := h.r.Issuer(ctx)
	if i.Status.Frozen || len(i.Status.RecoveryRequests) != 0 {
		t.Fatal("dry run mutated epoch")
	}
	q.DryRun = false
	if _, e := h.c.Maintain(ctx, q, nil); e != nil {
		t.Fatal(e)
	}
	blob, e := h.c.Export(ctx)
	if e != nil {
		t.Fatal(e)
	}
	restore := protocol.RecoveryRequest{Action: "restore", Operator: "owner", Nonce: uuid.NewString()}
	if _, e = h.c.Maintain(ctx, restore, blob); e != nil {
		t.Fatal(e)
	}
	i, _ = h.r.Issuer(ctx)
	if len(i.Status.RecoveryRequests) != 2 || i.Status.RecoveryRequests[1] != restore {
		t.Fatal("restore discarded maintenance audit")
	}
	q.Action = "resume"
	if _, e = h.c.Maintain(ctx, q, nil); e == nil {
		t.Fatal("nonce reuse with changed action accepted")
	}
	q.Nonce = uuid.NewString()
	if _, e = h.c.Maintain(ctx, q, nil); e != nil {
		t.Fatal(e)
	}
	i, _ = h.r.Issuer(ctx)
	if i.Status.Frozen || len(i.Status.RecoveryRequests) != 3 {
		t.Fatal("resume missing audited request")
	}
}

type freezeObserver struct {
	registry.Backend
	frozen chan struct{}
	once   sync.Once
}

func (b *freezeObserver) CAS(ctx context.Context, d registry.Document, version string) (registry.Document, error) {
	out, e := b.Backend.CAS(ctx, d, version)
	if e == nil && d.Kind == registry.KindIssuer {
		var s registry.IssuerStatus
		_ = json.Unmarshal(d.Status, &s)
		if s.Frozen {
			b.once.Do(func() { close(b.frozen) })
		}
	}
	return out, e
}
func TestFreezeDrainsInFlightMutationBeforeExport(t *testing.T) {
	backend := &freezeObserver{Backend: registry.NewMemoryBackend(), frozen: make(chan struct{})}
	h := setupBackend(t, true, backend)
	v := create(t, h, "pvc-a")
	entered := make(chan struct{})
	resume := make(chan struct{})
	h.api.attachHook = func() { close(entered); <-resume }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	published := make(chan error, 1)
	go func() {
		_, e := h.c.Publish(ctx, h.r.Handle(v.Status.ProviderID), EncodeNodeID(h.node), false)
		published <- e
	}()
	<-entered
	frozen := make(chan error, 1)
	go func() { frozen <- h.c.Freeze(ctx) }()
	<-backend.frozen
	select {
	case e := <-frozen:
		t.Fatal("freeze returned with in-flight mutation", e)
	default:
	}
	if _, e := h.c.EnsureVolume(ctx, request("blocked")); e == nil {
		t.Fatal("freeze admitted new mutation")
	}
	close(resume)
	if e := <-published; e != nil {
		t.Fatal(e)
	}
	if e := <-frozen; e != nil {
		t.Fatal(e)
	}
	blob, e := h.c.Export(ctx)
	if e != nil || len(blob) == 0 {
		t.Fatal("drained export failed", e)
	}
	saved, _ := h.r.Volume(ctx, v.Name)
	if saved.Status.Serial == "" || saved.Status.PendingOp != "" {
		t.Fatal("snapshot raced the operation result")
	}
	if e = h.c.Resume(ctx); e != nil {
		t.Fatal(e)
	}
	_ = create(t, h, "after-resume")
}

func TestStaleCompletionCannotModifyCurrentOperation(t *testing.T) {
	h := setup(t, true)
	ctx := context.Background()
	v := create(t, h, "pvc-a")
	h.api.attachErr = context.DeadlineExceeded
	_, e := h.c.Publish(ctx, h.r.Handle(v.Status.ProviderID), EncodeNodeID(h.node), false)
	mustBlocked(t, e)
	v, _ = h.r.Volume(ctx, v.Name)
	op, _ := h.r.Operation(ctx, v.Status.PendingOp)
	op.UID = "previous-object-uid"
	if e = h.c.finish(ctx, op, "late old callback"); e == nil {
		t.Fatal("stale completion accepted")
	}
	v, _ = h.r.Volume(ctx, v.Name)
	if v.Status.PendingOp == "" {
		t.Fatal("stale completion released lock")
	}
}
func TestStaleEnrollmentCannotModifyRestoredRecord(t *testing.T) {
	h := setup(t, true)
	ctx := context.Background()
	v := create(t, h, "pvc-a")
	h.channel.observeErr = true
	_, e := h.c.Publish(ctx, h.r.Handle(v.Status.ProviderID), EncodeNodeID(h.node), false)
	mustBlocked(t, e)
	h.channel.observeErr = false
	v, _ = h.r.Volume(ctx, v.Name)
	op, _ := h.r.Operation(ctx, v.Status.PendingOp)
	op.UID = "previous-object-uid"
	if e = h.c.completeEnrollment(ctx, op); e == nil {
		t.Fatal("stale enrollment accepted")
	}
	v, _ = h.r.Volume(ctx, v.Name)
	if v.Status.Serial != "" || v.Status.PendingOp == "" {
		t.Fatal("stale enrollment changed restored ownership")
	}
}

func TestReplacementCannotResumeWhileOldIssuerAlive(t *testing.T) {
	h := setup(t, true)
	ctx := context.Background()
	if e := h.c.Freeze(ctx); e != nil {
		t.Fatal(e)
	}
	next := New(h.r, h.api, h.channel, h.cfg)
	_ = next.StartIssuer(ctx, controllerPod, "replacement")
	if e := next.Resume(ctx); e == nil {
		t.Fatal("replacement unfroze live old issuer")
	}
	f, _ := h.r.Frozen(ctx)
	if !f {
		t.Fatal("failed resume cleared freeze")
	}
	h.channel.verdict = protocol.Gone
	if e := next.Resume(ctx); e != nil {
		t.Fatal(e)
	}
	current, _ := h.r.Issuer(ctx)
	if current.Status.Current.ProcessNonce != "replacement" {
		t.Fatal("resume did not establish new verified issuer")
	}
}

func TestLostEpochResumeRequiresProjectFenceAndExclusiveHost(t *testing.T) {
	h := setup(t, true)
	ctx := context.Background()
	if e := h.c.Freeze(ctx); e != nil {
		t.Fatal(e)
	}
	epoch, e := h.r.Issuer(ctx)
	if e != nil {
		t.Fatal(e)
	}
	epoch.Status.RestoreNeedsFence = true
	if _, e = h.r.SaveIssuer(ctx, epoch, epoch.Version); e != nil {
		t.Fatal(e)
	}
	unknown := h.c.current()
	unknown.ProcessNonce = "unknown-newer-process"
	unknown.HostPID = 99
	unknown.StartTicks = 9999
	h.channel.extraIssuers = []protocol.IssuerDescriptor{unknown}
	if e = h.c.Resume(ctx); e == nil {
		t.Fatal("unproven host process admitted after epoch loss")
	}
	h.channel.extraIssuers = nil
	h.api.extraServers = []cloud.Server{{ID: "ffffffff-ffff-4fff-8fff-ffffffffffff", ProjectID: projectID, Name: "other-managed-vm", Status: "ACTIVE", SwitchStatus: "ON"}}
	if e = h.c.Resume(ctx); e == nil {
		t.Fatal("another active VM admitted after epoch loss")
	}
	h.api.extraServers[0].Status = "STOPPED"
	h.api.extraServers[0].SwitchStatus = "OFF"
	if e = h.c.Resume(ctx); e != nil {
		t.Fatal("verified disaster recovery could not resume", e)
	}
	epoch, e = h.r.Issuer(ctx)
	if e != nil || epoch.Status.RestoreNeedsFence || epoch.Status.Frozen || !h.c.Ready(ctx) || h.api.powerCalls != 0 {
		t.Fatal("recovery guard or manual power policy violated", e)
	}
}
