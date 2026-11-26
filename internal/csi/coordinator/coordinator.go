// Package coordinator is the only authority allowed to issue storage mutations.
package coordinator

import (
	"context"
	"errors"
	"github.com/RaveTechnology/clo-cloud-go/cloud"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"github.com/google/uuid"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

type Config struct {
	ProjectID         string
	ControllerNodeUID string
	PollInterval      time.Duration
	OperationTimeout  time.Duration
	MaxVolumesPerNode int
}
type Coordinator struct {
	repo         *registry.Repository
	api          cloud.StorageAPI
	nodes        protocol.Channel
	cfg          Config
	mu           sync.RWMutex
	issuer       protocol.IssuerDescriptor
	launchPodUID string
	launchNonce  string
	ready        bool
	workMu       sync.Mutex
	active       int
	stopping     bool
	drained      chan struct{}
	preparingMu  sync.Mutex
	preparations map[string]struct{}
	proofMu      sync.Mutex
}

func New(repo *registry.Repository, api cloud.StorageAPI, nodes protocol.Channel, cfg Config) *Coordinator {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.OperationTimeout <= 0 {
		cfg.OperationTimeout = 3 * time.Minute
	}
	if cfg.MaxVolumesPerNode <= 0 {
		cfg.MaxVolumesPerNode = 8
	}
	return &Coordinator{repo: repo, api: api, nodes: nodes, cfg: cfg}
}
func (c *Coordinator) Repository() *registry.Repository { return c.repo }
func (c *Coordinator) current() protocol.IssuerDescriptor {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.issuer
}
func (c *Coordinator) Ready(ctx context.Context) bool { return c.checkIssuer(ctx) == nil }
func (c *Coordinator) checkIssuer(ctx context.Context) error {
	c.mu.RLock()
	ready, issuer := c.ready, c.issuer
	c.mu.RUnlock()
	if !ready {
		return protocol.Fail(protocol.Unavailable, "issuer registration pending")
	}
	active, e := c.repo.Issuer(ctx)
	if e != nil {
		return e
	}
	if active.Status.Current != issuer || active.Status.Frozen || active.Status.RestoreNeedsFence {
		return protocol.Fail(protocol.Blocked, "issuer inactive or writers frozen")
	}
	return nil
}
func (c *Coordinator) project(ctx context.Context) error {
	if c.cfg.ProjectID != c.repo.Installation().ProjectID {
		return protocol.Fail(protocol.Invalid, "configuration project differs from installation")
	}
	p, e := c.api.GetProject(ctx, c.cfg.ProjectID)
	if e != nil {
		return protocol.Fail(protocol.Unavailable, "CLO project access unverified")
	}
	if p.ID != c.cfg.ProjectID || strings.ToUpper(p.Status) != "ACTIVE" {
		return protocol.Fail(protocol.Blocked, "CLO project inactive or different")
	}
	return nil
}
func (c *Coordinator) inventory(ctx context.Context) ([]cloud.Volume, error) {
	if e := c.project(ctx); e != nil {
		return nil, e
	}
	vs, e := c.api.ListVolumes(ctx, c.cfg.ProjectID)
	if e != nil {
		return nil, protocol.Fail(protocol.Unavailable, "CLO project volume inventory unavailable")
	}
	ids := map[string]bool{}
	for _, v := range vs {
		if v.ID == "" || ids[v.ID] {
			return nil, protocol.Fail(protocol.RecoveryRequired, "duplicate or empty provider volume ID")
		}
		ids[v.ID] = true
	}
	return vs, nil
}
func (c *Coordinator) audit(ctx context.Context) error {
	vs, e := c.inventory(ctx)
	if e != nil {
		return e
	}
	owned := make([]registry.ProviderVolume, 0, len(vs))
	for _, v := range vs {
		owned = append(owned, registry.ProviderVolume{ID: v.ID, Name: v.Name})
	}
	return c.repo.AuditOwned(ctx, owned)
}
func (c *Coordinator) ownedVolume(ctx context.Context, v registry.VolumeRecord) (cloud.Volume, error) {
	vs, e := c.inventory(ctx)
	if e != nil {
		return cloud.Volume{}, e
	}
	for _, a := range vs {
		if a.ID != v.Status.ProviderID {
			continue
		}
		if a.Name != v.Spec.ProviderName || a.Bootable {
			return cloud.Volume{}, protocol.Fail(protocol.Blocked, "provider ownership/boot guard differs")
		}
		actual, e := c.api.GetVolume(ctx, a.ID)
		if e != nil {
			return cloud.Volume{}, protocol.Fail(protocol.Unavailable, "CLO volume detail unavailable")
		}
		if actual.ID != a.ID || actual.Name != a.Name || actual.Bootable {
			return cloud.Volume{}, protocol.Fail(protocol.Blocked, "provider volume identity differs")
		}
		return actual, nil
	}
	return cloud.Volume{}, protocol.Fail(protocol.NotFound, "owned volume absent from verified project")
}
func (c *Coordinator) exchange(ctx context.Context, typ string, n protocol.NodeRef, v protocol.VolumeRef, issuer protocol.IssuerDescriptor, pod, nonce string) (protocol.Receipt, error) {
	job := protocol.Job{ID: uuid.NewString(), Nonce: uuid.NewString(), Type: typ, Node: n, Volume: v, Issuer: issuer, PodUID: pod, ProcessNonce: nonce}
	job.InstallationID = c.repo.Installation().ID
	return c.exchangePrepared(ctx, job)
}

func (c *Coordinator) exchangePrepared(ctx context.Context, job protocol.Job) (protocol.Receipt, error) {
	r, e := c.nodes.Exchange(ctx, job)
	if e != nil {
		return r, protocol.Fail(protocol.Unavailable, "bound node job unavailable: %s", job.Type)
	}
	if r.JobID != job.ID || r.Nonce != job.Nonce || r.Type != job.Type || r.Node != job.Node || r.Volume != job.Volume || r.Error != "" {
		return r, protocol.Fail(protocol.Blocked, "node receipt identity differs")
	}
	return r, nil
}
func (c *Coordinator) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.cfg.OperationTimeout)
}
func code(err error, want protocol.Code) bool {
	var e *protocol.Error
	return errors.As(err, &e) && e.Code == want
}
func sorted(ids []string) []string {
	out := append([]string(nil), ids...)
	sort.Strings(out)
	return out
}
func sameIDs(a, b []string) bool { return reflect.DeepEqual(sorted(a), sorted(b)) }
func volumeRef(r *registry.Repository, v registry.VolumeRecord) protocol.VolumeRef {
	return protocol.VolumeRef{RecordID: v.Name, Handle: r.Handle(v.Status.ProviderID), ProviderID: v.Status.ProviderID, Serial: v.Status.Serial, FSUUID: v.Status.FSUUID, AttachEpoch: v.Status.AttachEpoch}
}
func (c *Coordinator) trackPreparation(name string) func() {
	c.preparingMu.Lock()
	if c.preparations == nil {
		c.preparations = make(map[string]struct{})
	}
	c.preparations[name] = struct{}{}
	c.preparingMu.Unlock()
	return func() {
		c.preparingMu.Lock()
		delete(c.preparations, name)
		c.preparingMu.Unlock()
	}
}

func (c *Coordinator) preparing(name string) bool {
	c.preparingMu.Lock()
	defer c.preparingMu.Unlock()
	_, ok := c.preparations[name]
	return ok
}

func (c *Coordinator) newOperation(ctx context.Context, typ string, v registry.VolumeRecord, n *registry.NodeRecord, before protocol.Snapshot, apiIDs []string, targetGB int, limits ...int64) (registry.OperationRecord, func(), error) {
	if e := c.checkIssuer(ctx); e != nil {
		return registry.OperationRecord{}, func() {}, e
	}
	s := registry.OperationSpec{Type: typ, Nonce: uuid.NewString(), VolumeRecordID: v.Name, Issuer: c.current(), Before: before, APIVolumeIDs: sorted(apiIDs), TargetSizeGB: targetGB}
	if len(limits) > 0 {
		s.LimitBytes = limits[0]
	}
	if n != nil {
		s.NodeBindingID = n.Name
		if typ == "detach" && n.Status.Fenced {
			s.FenceID = n.Status.FenceID
		}
	}
	if deadline, ok := ctx.Deadline(); ok {
		s.Deadline = deadline
	}
	// Register before durable creation: recovery must not overtake acquisition
	// of either claim or the caller's following dispatch metadata writes.
	finish := c.trackPreparation(registry.Name("o", s.Nonce))
	prepared := false
	defer func() {
		if !prepared {
			finish()
		}
	}()
	op, e := c.repo.CreateOperation(ctx, s)
	if e != nil {
		return op, finish, e
	}
	if n != nil {
		if e = c.repo.LockNode(ctx, n.Name, op.Name, n.Version); e != nil {
			c.abortPrepared(ctx, op)
			return op, finish, e
		}
	}
	if e = c.repo.LockVolume(ctx, v.Name, op.Name, v.Version); e != nil {
		c.abortPrepared(ctx, op)
		return op, finish, e
	}
	prepared = true
	return op, finish, nil
}
func (c *Coordinator) abortPrepared(ctx context.Context, op registry.OperationRecord) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	fresh, e := c.operationIdentity(ctx, op)
	if e != nil || fresh.Status.Sent || fresh.Status.Phase != registry.Prepared || fresh.UID != op.UID {
		return
	}
	fresh.Status.Phase = registry.Completed
	fresh.Status.Result = "aborted before dispatch"
	saved, e := c.repo.SaveOperation(ctx, fresh, fresh.Version)
	if e == nil {
		_ = c.repo.Release(ctx, saved)
	}
}
func (c *Coordinator) send(ctx context.Context, op registry.OperationRecord, mutation func() error) error {
	if e := c.operationAuthority(ctx, op); e != nil {
		return e
	}
	if e := c.audit(ctx); e != nil {
		return e
	}
	g, e := c.repo.Dispatch(ctx, op.Name, c.current())
	if e != nil {
		return e
	}
	if e = g.Send(c.current(), mutation); e != nil {
		c.uncertain(ctx, op, "provider result uncertain")
		return protocol.Fail(protocol.RecoveryRequired, "provider result uncertain; inspect operation %s", op.Name)
	}
	return nil
}
func (c *Coordinator) uncertain(ctx context.Context, op registry.OperationRecord, reason string) {
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	fresh, e := c.repo.Operation(writeCtx, op.Name)
	if e != nil || fresh.UID != op.UID {
		return
	}
	fresh.Status.Phase = registry.Uncertain
	fresh.Status.Reason = reason
	_, _ = c.repo.SaveOperation(writeCtx, fresh, fresh.Version)
}
func (c *Coordinator) finish(ctx context.Context, op registry.OperationRecord, result string) error {
	if e := c.operationAuthority(ctx, op); e != nil {
		return e
	}
	fresh, e := c.repo.Operation(ctx, op.Name)
	if e != nil {
		return e
	}
	fresh.Status.Phase = registry.Completed
	fresh.Status.Result = result
	fresh.Status.Reason = ""
	fresh, e = c.repo.SaveOperation(ctx, fresh, fresh.Version)
	if e != nil {
		return e
	}
	return c.repo.Release(ctx, fresh)
}
func (c *Coordinator) pending(ctx context.Context, v registry.VolumeRecord, typ string) (bool, error) {
	if v.Status.PendingOp == "" {
		return false, nil
	}
	op, e := c.repo.Operation(ctx, v.Status.PendingOp)
	if e != nil {
		return true, e
	}
	if op.Status.Phase == registry.Completed {
		return true, c.repo.Release(ctx, op)
	}
	if op.Spec.Type != typ {
		return true, protocol.Fail(protocol.Conflict, "volume has pending %s operation %s", op.Spec.Type, op.Name)
	}
	return true, c.reconcile(ctx, op)
}
func (c *Coordinator) waitVolume(ctx context.Context, v registry.VolumeRecord, ready func(cloud.Volume) bool) (cloud.Volume, error) {
	var last cloud.Volume
	e := cloud.Wait(ctx, c.cfg.PollInterval, func(ctx context.Context) (string, error) {
		actual, e := c.ownedVolume(ctx, v)
		if code(e, protocol.NotFound) {
			return "PENDING", nil
		}
		if e != nil {
			return "", e
		}
		last = actual
		if ready(actual) {
			return "READY", nil
		}
		return actual.Status, nil
	}, func(s string) bool { return s == "READY" })
	return last, e
}
