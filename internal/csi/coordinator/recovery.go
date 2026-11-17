package coordinator

import (
	"context"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"github.com/google/uuid"
	"reflect"
	"strings"
)

func auditRequest(existing []protocol.RecoveryRequest, q protocol.RecoveryRequest) ([]protocol.RecoveryRequest, error) {
	for _, old := range existing {
		if old.Nonce == q.Nonce {
			if !reflect.DeepEqual(old, q) {
				return nil, protocol.Fail(protocol.Conflict, "recovery nonce reused with different input")
			}
			return existing, nil
		}
	}
	if len(existing) >= 1000 {
		return nil, protocol.Fail(protocol.Blocked, "recovery audit capacity reached; export registry before maintenance")
	}
	return append(existing, q), nil
}
func (c *Coordinator) Recover(ctx context.Context, q protocol.RecoveryRequest) (registry.OperationRecord, error) {
	done, admitErr := c.begin(ctx)
	if admitErr != nil {
		return registry.OperationRecord{}, admitErr
	}
	defer done()
	var out registry.OperationRecord
	if strings.TrimSpace(q.Operator) == "" || len(q.Operator) > 128 {
		return out, protocol.Fail(protocol.Invalid, "explicit operator required")
	}
	if _, e := uuid.Parse(q.Nonce); e != nil {
		return out, protocol.Fail(protocol.Invalid, "recovery UUID nonce required")
	}
	if frozen, e := c.repo.Frozen(ctx); e == nil && frozen {
		return out, protocol.Fail(protocol.Blocked, "registry writers frozen")
	} else if e != nil && !code(e, protocol.NotFound) {
		return out, e
	}
	// This read-only-cloud admin action must work before issuer readiness, so a
	// lost controller host cannot deadlock the explicit VM fencing procedure.
	if q.Action == "record-fence" {
		return c.recordFence(ctx, q)
	}
	if e := c.checkIssuer(ctx); e != nil {
		return out, e
	}
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	switch q.Action {
	case "reconcile":
		op, e := c.repo.Operation(ctx, q.OperationID)
		if e != nil {
			return out, e
		}
		if q.DryRun {
			_, e = c.reconciliation(ctx, op)
			return op, e
		}
		op.Status.RecoveryRequests, e = auditRequest(op.Status.RecoveryRequests, q)
		if e != nil {
			return out, e
		}
		op, e = c.repo.SaveOperation(ctx, op, op.Version)
		if e != nil {
			return out, e
		}
		if e = c.reconcile(ctx, op); e != nil {
			return op, e
		}
		return c.repo.Operation(ctx, op.Name)
	case "clear-quarantine":
		n, e := c.repo.Node(ctx, q.BindingID)
		if e != nil {
			return out, e
		}
		if n.Status.Fenced || n.Status.PendingEnrollment != "" {
			return out, protocol.Fail(protocol.Blocked, "fenced/pending node cannot clear quarantine")
		}
		ops, e := c.repo.Operations(ctx)
		if e != nil {
			return out, e
		}
		for _, op := range ops {
			if op.Spec.NodeBindingID == n.Name && op.Status.Phase != registry.Completed {
				return out, protocol.Fail(protocol.Blocked, "node has unresolved operation %s", op.Name)
			}
		}
		r, e := c.exchange(ctx, "observe", n.Spec.Ref, protocol.VolumeRef{}, protocol.IssuerDescriptor{}, "", "")
		if e != nil {
			return out, e
		}
		if _, e = c.checkInventory(ctx, n, r.Snapshot, ""); e != nil {
			return out, e
		}
		vs, e := c.repo.Volumes(ctx)
		if e != nil {
			return out, e
		}
		for _, v := range vs {
			if v.Status.NodeBindingID != n.Name {
				continue
			}
			if v.Status.PendingOp != "" {
				return out, protocol.Fail(protocol.Blocked, "volume still pending")
			}
			for _, d := range r.Snapshot.Disks {
				if d.Serial != v.Status.Serial {
					continue
				}
				if v.Status.Initialization == "FormatComplete" {
					if d.FSType != "ext4" || d.FSUUID != v.Status.FSUUID {
						return out, protocol.Fail(protocol.Blocked, "filesystem identity differs")
					}
				} else if d.FSType != "" || d.FSUUID != "" {
					return out, protocol.Fail(protocol.Blocked, "unresolved filesystem signature")
				}
				for _, m := range d.Mounts {
					if !strings.HasPrefix(m.Target, "/var/lib/kubelet/") {
						return out, protocol.Fail(protocol.Blocked, "foreign mount reference")
					}
				}
			}
		}
		if q.DryRun {
			return out, nil
		}
		n.Status.RecoveryRequests, e = auditRequest(n.Status.RecoveryRequests, q)
		if e != nil {
			return out, e
		}
		n.Status.Quarantined = false
		n.Status.Reason = ""
		if _, e = c.repo.SaveNode(ctx, n, n.Version); e != nil {
			return out, e
		}
		for _, v := range vs {
			if v.Status.NodeBindingID == n.Name && v.Status.Quarantined {
				v.Status.Quarantined = false
				v.Status.Reason = ""
				if _, e = c.repo.SaveVolume(ctx, v, v.Version); e != nil {
					return out, e
				}
			}
		}
		return out, nil
	case "retire-unpublished":
		v, e := c.repo.Volume(ctx, q.VolumeID)
		if e != nil {
			return out, e
		}
		if v.Status.Published || v.Status.Initialization != "" || v.Status.FormatOp != "" || v.Status.NodeBindingID != "" {
			return out, protocol.Fail(protocol.Blocked, "only never-published/unformatted volumes can retire")
		}
		if v.Status.PendingOp != "" {
			pending, e := c.repo.Operation(ctx, v.Status.PendingOp)
			if e != nil {
				return out, e
			}
			if pending.Spec.Type != "create" {
				return out, protocol.Fail(protocol.Blocked, "retirement cannot close non-create operation")
			}
		}
		if q.DryRun {
			return out, nil
		}
		v.Status.RecoveryRequests, e = auditRequest(v.Status.RecoveryRequests, q)
		if e != nil {
			return out, e
		}
		v.Status.Retired = true
		v, e = c.repo.SaveVolume(ctx, v, v.Version)
		if e != nil {
			return out, e
		}
		if v.Status.PendingOp != "" {
			op, e := c.repo.Operation(ctx, v.Status.PendingOp)
			if e != nil {
				return out, e
			}
			if op.Spec.Type != "create" {
				return out, protocol.Fail(protocol.Blocked, "retirement cannot close non-create operation")
			}
			if e = c.finish(ctx, op, "explicitly retired; provider name tombstone retained"); e != nil {
				return out, e
			}
			return c.repo.Operation(ctx, op.Name)
		}
		return out, nil
	case "recover-attachment":
		return c.recoverAttachment(ctx, q)
	default:
		return out, protocol.Fail(protocol.Invalid, "unsupported recovery action")
	}
}
func (c *Coordinator) priorIssuerGone(ctx context.Context, op registry.OperationRecord) error {
	if op.Spec.Issuer == c.current() {
		return nil
	}
	active, e := c.repo.Issuer(ctx)
	if e != nil {
		return e
	}
	for _, p := range active.Status.Prior {
		if p.Descriptor == op.Spec.Issuer && p.Verdict == protocol.Gone {
			return nil
		}
	}
	return protocol.Fail(protocol.Blocked, "operation issuer termination not proven")
}

// reconciliation observes terminal postconditions first and returns a write-only
// closure. Dry-run executes the same observations without any record changes.
func (c *Coordinator) reconciliation(ctx context.Context, op registry.OperationRecord) (func() error, error) {
	if e := c.priorIssuerGone(ctx, op); e != nil {
		return nil, e
	}
	if op.Status.Phase == registry.Completed {
		if !op.Status.Sent && op.Status.Result == "proven not dispatched" {
			return func() error { return c.completeUnsentOperation(ctx, op) }, nil
		}
		return func() error { return c.repo.Release(ctx, op) }, nil
	}
	v, e := c.repo.Volume(ctx, op.Spec.VolumeRecordID)
	if e != nil {
		return nil, e
	}
	if !op.Status.Sent && op.Status.Phase == registry.Prepared {
		return func() error { return c.completeUnsentOperation(ctx, op) }, nil
	}
	switch op.Spec.Type {
	case "create":
		actual, e := c.inventory(ctx)
		if e != nil {
			return nil, e
		}
		matches := 0
		for _, a := range actual {
			if a.Name != v.Spec.ProviderName {
				continue
			}
			matches++
			if _, e := uuid.Parse(a.ID); e != nil {
				return nil, protocol.Fail(protocol.RecoveryRequired, "create match ID is invalid")
			}
			if e := actualCapacity(a.SizeGB, op.Spec.LimitBytes); e != nil {
				return nil, e
			}
			if a.Bootable || a.AttachedServerID != "" || a.Status != "AVAILABLE" || a.SizeGB < op.Spec.TargetSizeGB {
				return nil, protocol.Fail(protocol.RecoveryRequired, "create match not a terminal own free volume")
			}
			if v.Status.ProviderID != "" && v.Status.ProviderID != a.ID {
				return nil, protocol.Fail(protocol.Blocked, "create match differs from saved ID")
			}
			v.Status.ProviderID = a.ID
			v.Status.FSUUID = a.ID
			v.Status.SizeGB = a.SizeGB
		}
		if matches != 1 {
			return nil, protocol.Fail(protocol.RecoveryRequired, "uncertain create has %d exact matches; no resend permitted", matches)
		}
		return func() error {
			if _, e := c.repo.SaveVolume(ctx, v, v.Version); e != nil {
				return e
			}
			return c.finish(ctx, op, "reconciled exact create match")
		}, nil
	case "attach":
		v, n, serial, e := c.observeEnrollment(ctx, op)
		if e != nil {
			return nil, e
		}
		return func() error {
			v.Status.Serial = serial
			v.Status.NodeBindingID = n.Name
			v.Status.Published = true
			if _, e := c.repo.SaveVolume(ctx, v, v.Version); e != nil {
				return e
			}
			return c.finish(ctx, op, "reconciled serial enrollment")
		}, nil
	case "detach":
		a, e := c.ownedVolume(ctx, v)
		if e != nil {
			return nil, e
		}
		if a.AttachedServerID != "" || a.Status != "AVAILABLE" {
			return nil, protocol.Fail(protocol.RecoveryRequired, "detach terminal postcondition absent; no resend")
		}
		return func() error { return c.completeDetach(ctx, op) }, nil
	case "extend":
		a, e := c.ownedVolume(ctx, v)
		if e != nil {
			return nil, e
		}
		if a.SizeGB < op.Spec.TargetSizeGB || (a.Status != "AVAILABLE" && a.Status != "IN_USE") {
			return nil, protocol.Fail(protocol.RecoveryRequired, "extend terminal postcondition absent; no resend")
		}
		if e := actualCapacity(a.SizeGB, op.Spec.LimitBytes); e != nil {
			return nil, e
		}
		return func() error {
			v.Status.SizeGB = a.SizeGB
			if _, e := c.repo.SaveVolume(ctx, v, v.Version); e != nil {
				return e
			}
			return c.finish(ctx, op, "reconciled extension")
		}, nil
	case "delete":
		gone, e := c.deleted(ctx, v)
		if e != nil {
			return nil, e
		}
		if !gone {
			return nil, protocol.Fail(protocol.RecoveryRequired, "delete terminal postcondition absent; no resend")
		}
		return func() error { return c.completeDelete(ctx, op) }, nil
	case "format":
		n, e := c.repo.Node(ctx, op.Spec.NodeBindingID)
		if e != nil {
			return nil, e
		}
		r, e := c.exchange(ctx, "verifyFormat", n.Spec.Ref, volumeRef(c.repo, v), protocol.IssuerDescriptor{}, "", "")
		if e != nil {
			return nil, e
		}
		if !r.FilesystemChecked {
			return nil, protocol.Fail(protocol.RecoveryRequired, "read-only filesystem check did not prove own filesystem")
		}
		r.JobID = op.Name
		r.Nonce = op.Spec.Nonce
		r.Type = "formatComplete"
		if _, _, d, e := c.stageFacts(ctx, protocol.StageRequest{Node: n.Spec.Ref, Volume: r.Volume, Snapshot: r.Snapshot}); e != nil {
			return nil, e
		} else if d.FSType != "ext4" || d.FSUUID != v.Status.FSUUID {
			return nil, protocol.Fail(protocol.RecoveryRequired, "format remains incomplete; no second mkfs permitted")
		}
		return func() error { return c.Receive(ctx, n.Spec.Ref, r) }, nil
	default:
		return nil, protocol.Fail(protocol.Invalid, "unsupported pending operation")
	}
}

// Close the exact Prepared version before resetting format state. Dispatch and
// cancellation race on the same CAS; once Completed, no old sender can acquire a
// grant. Keeping the result durable lets a restart finish reset/release safely.
func (c *Coordinator) completeUnsentOperation(ctx context.Context, op registry.OperationRecord) error {
	if e := c.operationAuthority(ctx, op); e != nil {
		return e
	}
	if op.Status.Sent || op.Status.Phase != registry.Prepared && (op.Status.Phase != registry.Completed || op.Status.Result != "proven not dispatched") {
		return protocol.Fail(protocol.Blocked, "operation is not proven unsent")
	}
	if op.Status.Phase == registry.Prepared {
		op.Status.Phase, op.Status.Result, op.Status.Reason = registry.Completed, "proven not dispatched", ""
		var e error
		op, e = c.repo.SaveOperation(ctx, op, op.Version)
		if e != nil {
			return e
		}
	}
	if op.Spec.Type == "format" {
		v, e := c.repo.Volume(ctx, op.Spec.VolumeRecordID)
		if e != nil {
			return e
		}
		if v.Status.FormatOp == op.Name {
			v.Status.FormatOp, v.Status.Initialization = "", ""
			if _, e = c.repo.SaveVolume(ctx, v, v.Version); e != nil {
				return e
			}
		}
	}
	return c.repo.Release(ctx, op)
}
func (c *Coordinator) reconcile(ctx context.Context, op registry.OperationRecord) error {
	apply, e := c.reconciliation(ctx, op)
	if e != nil {
		return e
	}
	return apply()
}
func (c *Coordinator) recoverAttachment(ctx context.Context, q protocol.RecoveryRequest) (registry.OperationRecord, error) {
	op, e := c.repo.Operation(ctx, q.OperationID)
	if e != nil {
		return op, e
	}
	if op.Spec.Type != "attach" && op.Spec.Type != "detach" {
		return op, protocol.Fail(protocol.Invalid, "only an attachment operation can recover with a fence")
	}
	if e = c.priorIssuerGone(ctx, op); e != nil {
		return op, e
	}
	n, e := c.repo.Node(ctx, op.Spec.NodeBindingID)
	if e != nil {
		return op, e
	}
	if q.FenceID == "" || q.FenceID != n.Status.FenceID {
		return op, protocol.Fail(protocol.Blocked, "matching explicit fence ID required")
	}
	if e = c.checkNodeFence(ctx, n); e != nil {
		return op, e
	}
	v, e := c.repo.Volume(ctx, op.Spec.VolumeRecordID)
	if e != nil {
		return op, e
	}
	a, e := c.ownedVolume(ctx, v)
	if e != nil {
		return op, e
	}
	if a.AttachedServerID != "" && (a.AttachedServerID != n.Spec.Ref.ServerID || a.Status != "IN_USE") {
		return op, protocol.Fail(protocol.RecoveryRequired, "pending cloud job/foreign target not resolved")
	}
	if a.AttachedServerID == "" && a.Status != "AVAILABLE" {
		return op, protocol.Fail(protocol.RecoveryRequired, "volume not terminal/free")
	}
	if q.DryRun {
		return op, nil
	}
	op.Status.RecoveryRequests, e = auditRequest(op.Status.RecoveryRequests, q)
	if e != nil {
		return op, e
	}
	op, e = c.repo.SaveOperation(ctx, op, op.Version)
	if e != nil {
		return op, e
	}
	v.Status.Quarantined = false
	v.Status.Reason = ""
	v.Status.Barrier = true
	if a.AttachedServerID != "" {
		v.Status.NodeBindingID = n.Name
	} else {
		v.Status.NodeBindingID = ""
	}
	if _, e = c.repo.SaveVolume(ctx, v, v.Version); e != nil {
		return op, e
	}
	if e = c.finish(ctx, op, "explicit fence accepted; original POST will not repeat"); e != nil {
		return op, e
	}
	if a.AttachedServerID != "" {
		if e = c.Unpublish(ctx, c.repo.Handle(v.Status.ProviderID), ""); e != nil {
			return op, e
		}
	}
	f, e := c.repo.Fence(ctx, n.Status.FenceID)
	if e != nil {
		return op, e
	}
	f.Status.Consumed = true
	if _, e = c.repo.SaveFence(ctx, f, f.Version); e != nil {
		return op, e
	}
	return c.repo.Operation(ctx, op.Name)
}
