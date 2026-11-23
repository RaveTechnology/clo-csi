package coordinator

import (
	"context"

	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
)

const retainedFormatResult = "incomplete initialization retired; original disk and format history retained"

// retireIncompleteFormat never grants a second mkfs and never mutates cloud or
// guest data. Its permanent tombstone also prevents a subsequent CSI Delete.
func (c *Coordinator) retireIncompleteFormat(ctx context.Context, q protocol.RecoveryRequest) (registry.OperationRecord, error) {
	op, e := c.repo.Operation(ctx, q.OperationID)
	if e != nil {
		return op, e
	}
	if op.Spec.Type != "format" || !op.Status.Sent || op.Spec.NodeBindingID == "" || c.preparing(op.Name) {
		return op, protocol.Fail(protocol.Blocked, "only an inactive dispatched format can retire")
	}
	if e = c.operationAuthority(ctx, op); e != nil {
		return op, e
	}
	v, e := c.repo.FindVolume(ctx, q.VolumeID)
	if e != nil {
		return op, e
	}
	if v.Name != op.Spec.VolumeRecordID || v.Status.Initialization != "FormatDispatched" || v.Status.FormatOp != op.Name || v.Status.Deleted {
		return op, protocol.Fail(protocol.Blocked, "exact incomplete volume handle and format history required")
	}
	completed := op.Status.Phase == registry.Completed
	if completed {
		if !v.Status.Retired || !v.Status.Barrier || op.Status.Result != retainedFormatResult {
			return op, protocol.Fail(protocol.Blocked, "completed format is not an incomplete retirement")
		}
	} else if v.Status.NodeBindingID != op.Spec.NodeBindingID || v.Status.PendingOp != op.Name {
		return op, protocol.Fail(protocol.Blocked, "incomplete format binding/claim differs")
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
	a, e := c.ownedVolume(ctx, v)
	if e != nil {
		return op, e
	}
	if a.AttachedServerID == "" && a.Status != "AVAILABLE" || a.AttachedServerID != "" && (a.AttachedServerID != n.Spec.Ref.ServerID || a.Status != "IN_USE") {
		return op, protocol.Fail(protocol.Blocked, "original disk is not terminal/free or on the fenced VM")
	}
	audit, e := auditRequest(op.Status.RecoveryRequests, q)
	if e != nil {
		return op, e
	}
	if q.DryRun {
		return op, nil
	}
	if e = c.operationAuthority(ctx, op); e != nil {
		return op, e
	}
	op.Status.RecoveryRequests = audit
	op, e = c.repo.SaveOperation(ctx, op, op.Version)
	if e != nil {
		return op, e
	}
	if !completed {
		v.Status.Barrier, v.Status.Retired = true, true
		if _, e = c.repo.SaveVolume(ctx, v, v.Version); e != nil {
			return op, e
		}
		if e = c.finish(ctx, op, retainedFormatResult); e != nil {
			return op, e
		}
	} else if e = c.repo.Release(ctx, op); e != nil {
		return op, e
	}
	return c.repo.Operation(ctx, op.Name)
}
