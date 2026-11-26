package coordinator

import (
	"context"

	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
)

const retainedQuiescedFormatResult = "quiesced incomplete initialization retired; original disk and format history retained"

func blankRetirementDevice(snapshot protocol.Snapshot, serial string) bool {
	found := false
	for _, d := range snapshot.Disks {
		if d.Serial != serial {
			continue
		}
		if found || d.Root || d.Partitioned || d.FSType != "" || d.FSUUID != "" || len(d.Mounts) != 0 {
			return false
		}
		found = true
	}
	return found
}

func (c *Coordinator) blankRetirementFacts(ctx context.Context, v registry.VolumeRecord, n registry.NodeRecord, snapshot protocol.Snapshot) error {
	if snapshot.BootID == "" || snapshot.BootID != n.Status.BootID || !blankRetirementDevice(snapshot, v.Status.Serial) {
		return protocol.Fail(protocol.Blocked, "current blank unmounted device and boot proof required")
	}
	actual, node, _, e := c.stageFacts(ctx, protocol.StageRequest{Node: n.Spec.Ref, Volume: volumeRef(c.repo, v), Snapshot: snapshot})
	if e != nil {
		return e
	}
	if actual.UID != v.UID || node.UID != n.UID || node.Status.BootID != snapshot.BootID {
		return protocol.Fail(protocol.Blocked, "volume/node incarnation changed during retirement proof")
	}
	server, e := c.ownedServer(ctx, node)
	if e != nil {
		return e
	}
	if server.Status != "ACTIVE" {
		return protocol.Fail(protocol.Blocked, "live retirement requires exact active VM")
	}
	return nil
}

// Once a tombstone exists, its preceding durable receipt proves revocation of
// the old grant. A completed retry may release only its old claims even after
// normal detach or a controller restart; it cannot re-open the volume.
func (c *Coordinator) savedQuiescedRetirement(op registry.OperationRecord, v registry.VolumeRecord, n registry.NodeRecord) bool {
	r := op.Status.Receipt
	if r == nil || r.Type != "quiesce" || r.JobID != op.Name || r.Nonce != op.Spec.Nonce || !r.Quiesced || r.Error != "" ||
		r.Node != n.Spec.Ref || r.Volume != volumeRef(c.repo, v) || r.Snapshot.BootID == "" || !blankRetirementDevice(r.Snapshot, v.Status.Serial) {
		return false
	}
	root, e := rootSerial(r.Snapshot)
	return e == nil && root == n.Status.RootSerial
}

// retireQuiescedFormat adds no cloud mutation and grants no new filesystem
// operation. Only the existing authenticated node quiescence may revoke the
// consumed initial permit. Lost or unverifiable nodes still require VM fencing.
func (c *Coordinator) retireQuiescedFormat(ctx context.Context, q protocol.RecoveryRequest) (registry.OperationRecord, error) {
	op, e := c.repo.Operation(ctx, q.OperationID)
	if e != nil {
		return op, e
	}
	if q.FenceID != "" || op.Spec.Type != "format" || !op.Status.Sent || op.Spec.NodeBindingID == "" || c.preparing(op.Name) {
		return op, protocol.Fail(protocol.Blocked, "only an inactive dispatched format without a VM fence can quiesce-retire")
	}
	if e = c.operationAuthority(ctx, op); e != nil {
		return op, e
	}
	v, e := c.repo.FindVolume(ctx, q.VolumeID)
	if e != nil {
		return op, e
	}
	if v.Name != op.Spec.VolumeRecordID || v.Status.Initialization != "FormatDispatched" || v.Status.FormatOp != op.Name || v.Status.Deleted || v.Status.Quarantined {
		return op, protocol.Fail(protocol.Blocked, "exact unused incomplete format handle/history required")
	}
	n, e := c.repo.Node(ctx, op.Spec.NodeBindingID)
	if e != nil {
		return op, e
	}
	audit, e := auditRequest(op.Status.RecoveryRequests, q)
	if e != nil {
		return op, e
	}
	completed := op.Status.Phase == registry.Completed
	if completed || v.Status.Retired {
		if !v.Status.Retired || !v.Status.Barrier || !c.savedQuiescedRetirement(op, v, n) || completed && op.Status.Result != retainedQuiescedFormatResult {
			return op, protocol.Fail(protocol.Blocked, "retirement tombstone has no matching durable quiescence proof")
		}
		if q.DryRun {
			return op, nil
		}
		op.Status.RecoveryRequests = audit
		op, e = c.repo.SaveOperation(ctx, op, op.Version)
		if e != nil {
			return op, e
		}
		if completed {
			e = c.repo.Release(ctx, op)
		} else {
			e = c.finish(ctx, op, retainedQuiescedFormatResult)
		}
		if e != nil {
			return op, e
		}
		return c.repo.Operation(ctx, op.Name)
	}
	if v.Status.NodeBindingID != n.Name || v.Status.PendingOp != op.Name || n.Status.PendingEnrollment != op.Name || n.Status.Fenced || n.Status.Quarantined {
		return op, protocol.Fail(protocol.Blocked, "live incomplete format binding/claims differ or node unavailable")
	}
	baseline, e := c.exchange(ctx, "baseline", n.Spec.Ref, volumeRef(c.repo, v), protocol.IssuerDescriptor{}, "", "")
	if e != nil {
		return op, e
	}
	if e = c.blankRetirementFacts(ctx, v, n, baseline.Snapshot); e != nil {
		return op, e
	}
	if q.DryRun {
		return op, nil
	}
	op.Status.RecoveryRequests = audit
	op, e = c.repo.SaveOperation(ctx, op, op.Version)
	if e != nil {
		return op, e
	}
	proof, e := c.exchange(ctx, "quiesce", n.Spec.Ref, volumeRef(c.repo, v), protocol.IssuerDescriptor{}, "", "")
	if e != nil {
		return op, e
	}
	if !proof.Quiesced || proof.Snapshot.BootID != baseline.Snapshot.BootID {
		return op, protocol.Fail(protocol.Blocked, "fresh durable local barrier and kernel quiescence required")
	}
	if e = c.blankRetirementFacts(ctx, v, n, proof.Snapshot); e != nil {
		return op, e
	}
	if e = c.operationAuthority(ctx, op); e != nil {
		return op, e
	}
	// Bind the authenticated job proof to the original operation before its
	// permanent tombstone. No format nonce or grant is reset or regenerated.
	proof.JobID, proof.Nonce = op.Name, op.Spec.Nonce
	op.Status.Receipt = &proof
	op, e = c.repo.SaveOperation(ctx, op, op.Version)
	if e != nil {
		return op, e
	}
	if e = c.operationAuthority(ctx, op); e != nil {
		return op, e
	}
	v.Status.Retired, v.Status.Barrier = true, true
	if _, e = c.repo.SaveVolume(ctx, v, v.Version); e != nil {
		return op, e
	}
	if e = c.finish(ctx, op, retainedQuiescedFormatResult); e != nil {
		return op, e
	}
	return c.repo.Operation(ctx, op.Name)
}
