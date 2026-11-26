package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/google/uuid"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
)

type Repository struct {
	backend      Backend
	installation Installation
	mu           sync.Mutex
	audited      bool
}

func New(b Backend, i Installation) *Repository  { return &Repository{backend: b, installation: i} }
func (r *Repository) Installation() Installation { return r.installation }
func Name(prefix, key string) string {
	h := sha256.Sum256([]byte(key))
	return prefix + "-" + hex.EncodeToString(h[:20])
}
func (r *Repository) Prefix() string {
	return "csi-" + strings.ReplaceAll(r.installation.ID, "-", "") + "-"
}
func (r *Repository) ProviderName(csiName string) string {
	h := sha256.Sum256([]byte(csiName))
	return r.Prefix() + hex.EncodeToString(h[:12])
}
func (r *Repository) Handle(providerID string) string {
	return "clo:" + r.installation.ID + ":" + providerID
}
func (r *Repository) ParseHandle(handle string) (string, error) {
	p := strings.Split(handle, ":")
	if len(p) != 3 || p[0] != "clo" || p[1] != r.installation.ID {
		return "", protocol.Fail(protocol.Invalid, "invalid volume handle")
	}
	if _, err := uuid.Parse(p[2]); err != nil {
		return "", protocol.Fail(protocol.Invalid, "invalid provider UUID")
	}
	return p[2], nil
}
func is(err error, code protocol.Code) bool {
	var e *protocol.Error
	return errors.As(err, &e) && e.Code == code
}
func (r *Repository) scope(installation, project string) error {
	if installation != r.installation.ID || project != r.installation.ProjectID {
		return protocol.Fail(protocol.RecoveryRequired, "registry scope mismatch")
	}
	return nil
}
func (r *Repository) valid() error {
	if _, err := uuid.Parse(r.installation.ID); err != nil {
		return protocol.Fail(protocol.Invalid, "invalid installation UUID")
	}
	if _, err := uuid.Parse(r.installation.ProjectID); err != nil {
		return protocol.Fail(protocol.Invalid, "invalid project UUID")
	}
	return nil
}

func (r *Repository) CreateVolume(ctx context.Context, s VolumeSpec) (VolumeRecord, error) {
	if err := r.valid(); err != nil {
		return VolumeRecord{}, err
	}
	if s.CSIName == "" || s.Fingerprint == "" {
		return VolumeRecord{}, protocol.Fail(protocol.Invalid, "volume owner is required")
	}
	s.InstallationID = r.installation.ID
	s.ProjectID = r.installation.ProjectID
	s.ProviderName = r.ProviderName(s.CSIName)
	d, err := r.backend.Create(ctx, document(KindVolume, Meta{Name: Name("v", s.CSIName)}, s, VolumeStatus{Initialized: true}))
	if err != nil {
		return VolumeRecord{}, err
	}
	return r.volume(d)
}
func (r *Repository) volume(d Document) (v VolumeRecord, err error) {
	v.Meta, err = decode(d, &v.Spec, &v.Status)
	if err == nil {
		err = r.scope(v.Spec.InstallationID, v.Spec.ProjectID)
	}
	if err == nil && (d.Name != Name("v", v.Spec.CSIName) || v.Spec.ProviderName != r.ProviderName(v.Spec.CSIName)) {
		err = protocol.Fail(protocol.RecoveryRequired, "volume owner hash mismatch")
	}
	return
}
func (r *Repository) Volume(ctx context.Context, name string) (VolumeRecord, error) {
	d, e := r.backend.Get(ctx, KindVolume, name)
	if e != nil {
		return VolumeRecord{}, e
	}
	return r.volume(d)
}
func (r *Repository) Volumes(ctx context.Context) ([]VolumeRecord, error) {
	ds, e := r.backend.List(ctx, KindVolume)
	if e != nil {
		return nil, e
	}
	out := make([]VolumeRecord, 0, len(ds))
	for _, d := range ds {
		v, e := r.volume(d)
		if e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, nil
}
func (r *Repository) FindVolume(ctx context.Context, handle string) (VolumeRecord, error) {
	id, e := r.ParseHandle(handle)
	if e != nil {
		return VolumeRecord{}, e
	}
	vs, e := r.Volumes(ctx)
	if e != nil {
		return VolumeRecord{}, e
	}
	var found *VolumeRecord
	for _, v := range vs {
		if v.Status.ProviderID == id {
			if found != nil {
				return VolumeRecord{}, protocol.Fail(protocol.RecoveryRequired, "duplicate provider ownership")
			}
			copy := v
			found = &copy
		}
	}
	if found == nil {
		return VolumeRecord{}, protocol.Fail(protocol.NotFound, "volume not registered")
	}
	return *found, nil
}
func immutable(old, next string) bool { return old == "" || old == next }
func (r *Repository) SaveVolume(ctx context.Context, v VolumeRecord, version string) (VolumeRecord, error) {
	old, e := r.Volume(ctx, v.Name)
	if e != nil {
		return VolumeRecord{}, e
	}
	if old.Status.Initialization == "FormatComplete" && v.Status.Initialization != "FormatComplete" {
		return VolumeRecord{}, protocol.Fail(protocol.Invalid, "completed filesystem initialization is immutable")
	}
	if old.Status.Retired && (!v.Status.Retired || old.Status.Barrier && !v.Status.Barrier) {
		return VolumeRecord{}, protocol.Fail(protocol.Invalid, "retirement and its existing barrier are permanent")
	}
	if old.Status.FormatOp != "" && (v.Status.FormatOp != old.Status.FormatOp || old.Status.Initialization == "FormatDispatched" && v.Status.Initialization == "") {
		op, e := r.Operation(ctx, old.Status.FormatOp)
		if e != nil {
			return VolumeRecord{}, e
		}
		if v.Status.FormatOp != "" || op.Status.Sent || op.Spec.Type != "format" || op.Status.Phase != Completed || op.Status.Result != "proven not dispatched" {
			return VolumeRecord{}, protocol.Fail(protocol.Invalid, "dispatched format cannot be reset")
		}
	}
	if !reflect.DeepEqual(old.Spec, v.Spec) || !immutable(old.Status.ProviderID, v.Status.ProviderID) || !immutable(old.Status.Serial, v.Status.Serial) || !immutable(old.Status.FSUUID, v.Status.FSUUID) || v.Status.AttachEpoch < old.Status.AttachEpoch || v.Status.SizeGB < old.Status.SizeGB || old.Status.Published && !v.Status.Published || old.Status.Deleted && !v.Status.Deleted || !v.Status.Initialized {
		return VolumeRecord{}, protocol.Fail(protocol.Invalid, "immutable volume identity or monotonic state changed")
	}
	d, e := r.backend.CAS(ctx, document(KindVolume, v.Meta, v.Spec, v.Status), version)
	if e != nil {
		return VolumeRecord{}, e
	}
	return r.volume(d)
}

func (r *Repository) CreateNode(ctx context.Context, s NodeSpec) (NodeRecord, error) {
	return r.CreateNodeWithStatus(ctx, s, NodeStatus{Initialized: true})
}

func (r *Repository) CreateNodeWithStatus(ctx context.Context, s NodeSpec, initial NodeStatus) (NodeRecord, error) {
	s.InstallationID = r.installation.ID
	s.ProjectID = r.installation.ProjectID
	if !initial.Initialized || s.Ref.UID == "" || s.Ref.ServerID == "" || s.Ref.ProjectID != s.ProjectID || s.Ref.Epoch < 1 || s.Identity.Validate() != nil {
		return NodeRecord{}, protocol.Fail(protocol.Invalid, "invalid node identity")
	}
	name := Name("n", s.Ref.UID+":"+s.Ref.ServerID)
	s.Ref.BindingID = name
	d, e := r.backend.Create(ctx, document(KindNode, Meta{Name: name}, s, initial))
	if e != nil {
		return NodeRecord{}, e
	}
	return r.node(d)
}
func (r *Repository) node(d Document) (v NodeRecord, err error) {
	v.Meta, err = decode(d, &v.Spec, &v.Status)
	if err == nil {
		err = r.scope(v.Spec.InstallationID, v.Spec.ProjectID)
	}
	return
}
func (r *Repository) Node(ctx context.Context, name string) (NodeRecord, error) {
	d, e := r.backend.Get(ctx, KindNode, name)
	if e != nil {
		return NodeRecord{}, e
	}
	return r.node(d)
}
func (r *Repository) Nodes(ctx context.Context) ([]NodeRecord, error) {
	ds, e := r.backend.List(ctx, KindNode)
	if e != nil {
		return nil, e
	}
	var out []NodeRecord
	for _, d := range ds {
		v, e := r.node(d)
		if e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, nil
}
func (r *Repository) SaveNode(ctx context.Context, v NodeRecord, version string) (NodeRecord, error) {
	old, e := r.Node(ctx, v.Name)
	if e != nil {
		return NodeRecord{}, e
	}
	if !reflect.DeepEqual(old.Spec, v.Spec) || !immutable(old.Status.RootSerial, v.Status.RootSerial) || old.Status.Fenced && !v.Status.Fenced || !v.Status.Initialized || len(old.Status.BootVolumeIDs) > 0 && !reflect.DeepEqual(old.Status.BootVolumeIDs, v.Status.BootVolumeIDs) {
		return NodeRecord{}, protocol.Fail(protocol.Invalid, "immutable node binding changed")
	}
	d, e := r.backend.CAS(ctx, document(KindNode, v.Meta, v.Spec, v.Status), version)
	if e != nil {
		return NodeRecord{}, e
	}
	return r.node(d)
}

func (r *Repository) CreateOperation(ctx context.Context, s OperationSpec) (OperationRecord, error) {
	s.InstallationID = r.installation.ID
	s.ProjectID = r.installation.ProjectID
	if s.Type == "" || s.Nonce == "" || !s.Issuer.Valid() {
		return OperationRecord{}, protocol.Fail(protocol.Invalid, "operation intent incomplete")
	}
	name := Name("o", s.Nonce)
	d, e := r.backend.Create(ctx, document(KindOperation, Meta{Name: name}, s, OperationStatus{Initialized: true, Phase: Prepared}))
	if e != nil {
		return OperationRecord{}, e
	}
	return r.operation(d)
}
func (r *Repository) operation(d Document) (v OperationRecord, err error) {
	v.Meta, err = decode(d, &v.Spec, &v.Status)
	if err == nil {
		err = r.scope(v.Spec.InstallationID, v.Spec.ProjectID)
	}
	return
}
func (r *Repository) Operation(ctx context.Context, name string) (OperationRecord, error) {
	d, e := r.backend.Get(ctx, KindOperation, name)
	if e != nil {
		return OperationRecord{}, e
	}
	return r.operation(d)
}
func (r *Repository) Operations(ctx context.Context) ([]OperationRecord, error) {
	ds, e := r.backend.List(ctx, KindOperation)
	if e != nil {
		return nil, e
	}
	var out []OperationRecord
	for _, d := range ds {
		v, e := r.operation(d)
		if e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, nil
}
func (r *Repository) SaveOperation(ctx context.Context, v OperationRecord, version string) (OperationRecord, error) {
	old, e := r.Operation(ctx, v.Name)
	if e != nil {
		return OperationRecord{}, e
	}
	if !reflect.DeepEqual(old.Spec, v.Spec) || old.Status.Sent && !v.Status.Sent || !v.Status.Initialized || old.Status.Phase == Completed && v.Status.Phase != Completed || old.Status.Sent && v.Status.Phase == Prepared {
		return OperationRecord{}, protocol.Fail(protocol.Invalid, "operation cannot be rewound")
	}
	d, e := r.backend.CAS(ctx, document(KindOperation, v.Meta, v.Spec, v.Status), version)
	if e != nil {
		return OperationRecord{}, e
	}
	return r.operation(d)
}
func (r *Repository) LockNode(ctx context.Context, node, op, expected string) error {
	o, e := r.Operation(ctx, op)
	if e != nil {
		return e
	}
	if o.Spec.NodeBindingID != node {
		return protocol.Fail(protocol.Invalid, "operation/node mismatch")
	}
	n, e := r.Node(ctx, node)
	if e != nil {
		return e
	}
	if expected == "" || n.Version != expected {
		return protocol.Fail(protocol.Conflict, "node changed since operation validation")
	}
	if n.Status.PendingEnrollment != "" && n.Status.PendingEnrollment != op {
		return protocol.Fail(protocol.Conflict, "node enrollment locked")
	}
	n.Status.PendingEnrollment = op
	_, e = r.SaveNode(ctx, n, n.Version)
	return e
}
func (r *Repository) LockVolume(ctx context.Context, volume, op, expected string) error {
	o, e := r.Operation(ctx, op)
	if e != nil {
		return e
	}
	if o.Spec.VolumeRecordID != volume {
		return protocol.Fail(protocol.Invalid, "operation/volume mismatch")
	}
	v, e := r.Volume(ctx, volume)
	if e != nil {
		return e
	}
	if expected == "" || v.Version != expected {
		return protocol.Fail(protocol.Conflict, "volume changed since operation validation")
	}
	if v.Status.PendingOp != "" && v.Status.PendingOp != op {
		return protocol.Fail(protocol.Conflict, "volume locked")
	}
	v.Status.PendingOp = op
	_, e = r.SaveVolume(ctx, v, v.Version)
	return e
}

func (r *Repository) Issuer(ctx context.Context) (IssuerRecord, error) {
	d, e := r.backend.Get(ctx, KindIssuer, "installation")
	if e != nil {
		return IssuerRecord{}, e
	}
	var v IssuerRecord
	v.Meta, e = decode(d, &v.Spec, &v.Status)
	if e == nil {
		e = r.scope(v.Spec.ID, v.Spec.ProjectID)
	}
	return v, e
}
func (r *Repository) SaveIssuer(ctx context.Context, v IssuerRecord, version string) (IssuerRecord, error) {
	if e := r.scope(v.Spec.ID, v.Spec.ProjectID); e != nil {
		return IssuerRecord{}, e
	}
	d, e := r.backend.CAS(ctx, document(KindIssuer, v.Meta, v.Spec, v.Status), version)
	if e != nil {
		return IssuerRecord{}, e
	}
	v.Meta, e = decode(d, &v.Spec, &v.Status)
	return v, e
}
func (r *Repository) ensureIssuer(ctx context.Context) (IssuerRecord, error) {
	v, e := r.Issuer(ctx)
	if !is(e, protocol.NotFound) {
		return v, e
	}
	d, e := r.backend.Create(ctx, document(KindIssuer, Meta{Name: "installation"}, r.installation, IssuerStatus{Initialized: true}))
	if e != nil {
		return IssuerRecord{}, e
	}
	v.Meta, e = decode(d, &v.Spec, &v.Status)
	return v, e
}
func (r *Repository) ActivateIssuer(ctx context.Context, next protocol.IssuerDescriptor, proof *protocol.IssuerProof) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !next.Valid() {
		return protocol.Fail(protocol.Invalid, "unverified issuer descriptor")
	}
	v, e := r.ensureIssuer(ctx)
	if e != nil {
		return e
	}
	old := v.Status.Current
	if v.Status.RestoreNeedsFence {
		return protocol.Fail(protocol.Blocked, "restored epoch requires verified recovery fencing")
	}
	if !old.Valid() {
		volumes, e := r.Volumes(ctx)
		if e != nil {
			return e
		}
		ops, e := r.Operations(ctx)
		if e != nil {
			return e
		}
		if len(volumes) > 0 || len(ops) > 0 {
			return protocol.Fail(protocol.RecoveryRequired, "issuer epoch missing while ownership records exist")
		}
	}
	if old == next {
		return nil
	}
	if old.Valid() {
		if proof == nil || proof.Descriptor != old || proof.Verdict != protocol.Gone {
			return protocol.Fail(protocol.Blocked, "previous issuer termination unproven")
		}
		v.Status.Prior = append(v.Status.Prior, *proof)
	}
	v.Status.Current = next
	_, e = r.SaveIssuer(ctx, v, v.Version)
	return e
}
func (r *Repository) SetFrozen(ctx context.Context, frozen bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, e := r.ensureIssuer(ctx)
	if e != nil {
		return e
	}
	v.Status.Frozen = frozen
	_, e = r.SaveIssuer(ctx, v, v.Version)
	return e
}
func (r *Repository) Frozen(ctx context.Context) (bool, error) {
	v, e := r.Issuer(ctx)
	return v.Status.Frozen, e
}

func (r *Repository) RecordMaintenance(ctx context.Context, q protocol.RecoveryRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, e := r.ensureIssuer(ctx)
	if e != nil {
		return e
	}
	for _, old := range v.Status.RecoveryRequests {
		if old.Nonce == q.Nonce {
			if old != q {
				return protocol.Fail(protocol.Conflict, "maintenance nonce reused with different input")
			}
			return nil
		}
	}
	if len(v.Status.RecoveryRequests) >= 1000 {
		return protocol.Fail(protocol.Blocked, "maintenance audit capacity reached")
	}
	v.Status.RecoveryRequests = append(v.Status.RecoveryRequests, q)
	_, e = r.SaveIssuer(ctx, v, v.Version)
	return e
}

// AuditOwned is required on every new coordinator incarnation before dispatch.
// It never adopts an unregistered provider volume, even with our name prefix.
func (r *Repository) AuditOwned(ctx context.Context, actual []ProviderVolume) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.audited = false
	vs, e := r.Volumes(ctx)
	if e != nil {
		return e
	}
	owners := map[string]VolumeRecord{}
	ids := map[string]string{}
	for _, v := range vs {
		if _, ok := owners[v.Spec.ProviderName]; ok {
			return protocol.Fail(protocol.RecoveryRequired, "duplicate provider name ownership")
		}
		owners[v.Spec.ProviderName] = v
		if id := v.Status.ProviderID; id != "" {
			if _, ok := ids[id]; ok {
				return protocol.Fail(protocol.RecoveryRequired, "duplicate provider ID ownership")
			}
			ids[id] = v.Name
		}
	}
	seen := map[string]bool{}
	for _, a := range actual {
		if !strings.HasPrefix(a.Name, r.Prefix()) {
			continue
		}
		v, ok := owners[a.Name]
		if !ok || seen[a.Name] || v.Status.ProviderID != "" && v.Status.ProviderID != a.ID {
			return protocol.Fail(protocol.RecoveryRequired, "owned prefix inventory differs from registry")
		}
		seen[a.Name] = true
	}
	r.audited = true
	return nil
}

type Grant struct {
	issuer protocol.IssuerDescriptor
	used   *atomic.Bool
}

func (g *Grant) Send(issuer protocol.IssuerDescriptor, send func() error) error {
	if issuer != g.issuer {
		return protocol.Fail(protocol.Blocked, "send issuer mismatch")
	}
	if !g.used.CompareAndSwap(false, true) {
		return protocol.Fail(protocol.Blocked, "send permission consumed")
	}
	return send()
}
func (r *Repository) Dispatch(ctx context.Context, name string, issuer protocol.IssuerDescriptor) (*Grant, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.audited {
		return nil, protocol.Fail(protocol.RecoveryRequired, "fresh project inventory audit required")
	}
	active, e := r.Issuer(ctx)
	if e != nil {
		return nil, e
	}
	if active.Status.Frozen || active.Status.RestoreNeedsFence || active.Status.Current != issuer {
		return nil, protocol.Fail(protocol.Blocked, "issuer inactive or writers frozen")
	}
	op, e := r.Operation(ctx, name)
	if e != nil {
		return nil, e
	}
	if op.Spec.Issuer != issuer || op.Status.Phase != Prepared || op.Status.Sent {
		return nil, protocol.Fail(protocol.Blocked, "operation send already consumed or foreign issuer")
	}
	if op.Spec.VolumeRecordID != "" {
		v, e := r.Volume(ctx, op.Spec.VolumeRecordID)
		if e != nil {
			return nil, e
		}
		if v.Status.PendingOp != name || v.Status.Quarantined {
			return nil, protocol.Fail(protocol.Blocked, "volume not exclusively claimed")
		}
	}
	if op.Spec.NodeBindingID != "" {
		n, e := r.Node(ctx, op.Spec.NodeBindingID)
		if e != nil {
			return nil, e
		}
		coldFencedDetach := false
		if op.Spec.Type == "detach" && n.Status.Fenced && op.Spec.FenceID != "" && op.Spec.FenceID == n.Status.FenceID {
			f, e := r.Fence(ctx, op.Spec.FenceID)
			if e != nil {
				return nil, e
			}
			coldFencedDetach = f.Status.Verified && f.Spec.BindingID == n.Name && f.Spec.ServerID == n.Spec.Ref.ServerID && f.Spec.NodeUID == n.Spec.Ref.UID && f.Spec.Epoch == n.Spec.Ref.Epoch
		}
		if n.Status.PendingEnrollment != name || (n.Status.Quarantined && !coldFencedDetach) {
			return nil, protocol.Fail(protocol.Blocked, "node not exclusively claimed")
		}
	}
	op.Status.Phase = Dispatched
	op.Status.Sent = true
	if _, e = r.SaveOperation(ctx, op, op.Version); e != nil {
		return nil, e
	}
	return &Grant{issuer: issuer, used: &atomic.Bool{}}, nil
}

func (r *Repository) CreateFence(ctx context.Context, s FenceSpec) (FenceRecord, error) {
	s.InstallationID = r.installation.ID
	s.ProjectID = r.installation.ProjectID
	if s.BindingID == "" || s.ServerID == "" || s.NodeUID == "" || s.Epoch < 1 || s.Nonce == "" || s.Operator == "" {
		return FenceRecord{}, protocol.Fail(protocol.Invalid, "incomplete explicit fence")
	}
	d, e := r.backend.Create(ctx, document(KindFence, Meta{Name: Name("f", s.Nonce)}, s, FenceStatus{Initialized: true}))
	if e != nil {
		return FenceRecord{}, e
	}
	return r.fence(d)
}
func (r *Repository) fence(d Document) (v FenceRecord, e error) {
	v.Meta, e = decode(d, &v.Spec, &v.Status)
	if e == nil {
		e = r.scope(v.Spec.InstallationID, v.Spec.ProjectID)
	}
	return
}
func (r *Repository) Fence(ctx context.Context, name string) (FenceRecord, error) {
	d, e := r.backend.Get(ctx, KindFence, name)
	if e != nil {
		return FenceRecord{}, e
	}
	return r.fence(d)
}
func (r *Repository) SaveFence(ctx context.Context, v FenceRecord, version string) (FenceRecord, error) {
	old, e := r.Fence(ctx, v.Name)
	if e != nil {
		return FenceRecord{}, e
	}
	if !reflect.DeepEqual(old.Spec, v.Spec) || old.Status.Consumed && !v.Status.Consumed || old.Status.Verified && !v.Status.Verified || !v.Status.Initialized {
		return FenceRecord{}, protocol.Fail(protocol.Invalid, "fence state cannot be rewound")
	}
	d, e := r.backend.CAS(ctx, document(KindFence, v.Meta, v.Spec, v.Status), version)
	if e != nil {
		return FenceRecord{}, e
	}
	return r.fence(d)
}

// Release only acknowledges a completed operation. Deadlines never unlock it.
func (r *Repository) Release(ctx context.Context, op OperationRecord) error {
	latest, e := r.Operation(ctx, op.Name)
	if e != nil {
		return e
	}
	if latest.UID != op.UID {
		return protocol.Fail(protocol.Conflict, "operation object recreated; stale release rejected")
	}
	if latest.Status.Phase != Completed {
		return protocol.Fail(protocol.Blocked, "operation unresolved")
	}
	if id := latest.Spec.VolumeRecordID; id != "" {
		v, e := r.Volume(ctx, id)
		if e != nil {
			return e
		}
		if v.Status.PendingOp == op.Name {
			v.Status.PendingOp = ""
			if _, e = r.SaveVolume(ctx, v, v.Version); e != nil {
				return e
			}
		}
	}
	if id := latest.Spec.NodeBindingID; id != "" {
		n, e := r.Node(ctx, id)
		if e != nil {
			return e
		}
		if n.Status.PendingEnrollment == op.Name {
			n.Status.PendingEnrollment = ""
			if _, e = r.SaveNode(ctx, n, n.Version); e != nil {
				return e
			}
		}
	}
	return nil
}
