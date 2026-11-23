package coordinator

import (
	"context"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
)

func (c *Coordinator) stageFacts(ctx context.Context, q protocol.StageRequest) (registry.VolumeRecord, registry.NodeRecord, protocol.Disk, error) {
	v, e := c.repo.FindVolume(ctx, q.Volume.Handle)
	if e != nil {
		return v, registry.NodeRecord{}, protocol.Disk{}, e
	}
	n, e := c.repo.Node(ctx, q.Node.BindingID)
	if e != nil {
		return v, n, protocol.Disk{}, e
	}
	if q.Node != n.Spec.Ref || q.Volume != volumeRef(c.repo, v) || v.Status.NodeBindingID != n.Name || v.Status.Serial == "" || v.Status.Barrier || v.Status.Quarantined || v.Status.Deleted || v.Status.Retired || n.Status.Fenced || n.Status.Quarantined {
		return v, n, protocol.Disk{}, protocol.Fail(protocol.Blocked, "stage generation/owner or barrier differs")
	}
	a, e := c.ownedVolume(ctx, v)
	if e != nil {
		return v, n, protocol.Disk{}, e
	}
	if a.AttachedServerID != n.Spec.Ref.ServerID || a.Status != "IN_USE" {
		return v, n, protocol.Disk{}, protocol.Fail(protocol.Blocked, "stage provider attachment differs")
	}
	if _, e = c.checkInventory(ctx, n, q.Snapshot, ""); e != nil {
		return v, n, protocol.Disk{}, e
	}
	var disk protocol.Disk
	for _, d := range q.Snapshot.Disks {
		if d.Serial == v.Status.Serial {
			if disk.Serial != "" || d.Root || d.Partitioned {
				return v, n, disk, protocol.Fail(protocol.Blocked, "stage disk is not uniquely eligible")
			}
			disk = d
		}
	}
	if disk.Serial == "" {
		return v, n, disk, protocol.Fail(protocol.Blocked, "stage serial missing")
	}
	return v, n, disk, nil
}
func (c *Coordinator) AuthorizeStage(ctx context.Context, q protocol.StageRequest) (protocol.StagePermit, error) {
	proofDone, proofErr := c.admitProof(ctx)
	if proofErr != nil {
		return protocol.StagePermit{}, proofErr
	}
	defer proofDone()
	done, admitErr := c.begin(ctx)
	if admitErr != nil {
		return protocol.StagePermit{}, admitErr
	}
	defer done()
	var out protocol.StagePermit
	if e := c.checkIssuer(ctx); e != nil {
		return out, e
	}
	v, n, d, e := c.stageFacts(ctx, q)
	if e != nil {
		return out, e
	}
	out = protocol.StagePermit{Volume: volumeRef(c.repo, v), Node: n.Spec.Ref, Initialization: v.Status.Initialization}
	if v.Status.FormatOp != "" {
		op, e := c.repo.Operation(ctx, v.Status.FormatOp)
		if e != nil {
			return out, e
		}
		out.OperationID = op.Name
		out.Nonce = op.Spec.Nonce
	}
	if !q.Format {
		if v.Status.PendingOp != "" && v.Status.PendingOp != v.Status.FormatOp {
			return out, protocol.Fail(protocol.Conflict, "volume has another pending operation")
		}
		out.ResumeQuiesced = v.Status.Initialization == "FormatComplete" && v.Status.PendingOp == ""
		return out, nil
	}
	if v.Status.Initialization != "" || v.Status.FormatOp != "" || v.Status.PendingOp != "" {
		return out, protocol.Fail(protocol.RecoveryRequired, "format permission was already consumed or volume is busy")
	}
	if d.FSType != "" || d.FSUUID != "" || len(d.Mounts) > 0 {
		return out, protocol.Fail(protocol.Blocked, "foreign signature or mount prevents initial format")
	}
	op, finishPreparation, e := c.newOperation(ctx, "format", v, &n, q.Snapshot, nil, 0)
	defer finishPreparation()
	if e != nil {
		return out, e
	}
	v, e = c.repo.Volume(ctx, v.Name)
	if e != nil {
		return out, e
	}
	v.Status.Initialization = "FormatDispatched"
	v.Status.FormatOp = op.Name
	v, e = c.repo.SaveVolume(ctx, v, v.Version)
	if e != nil {
		return out, e
	}
	if e = c.send(ctx, op, func() error { return nil }); e != nil {
		return out, e
	}
	out.Initialization = v.Status.Initialization
	out.OperationID = op.Name
	out.Nonce = op.Spec.Nonce
	out.AllowFormat = true
	return out, nil
}
func (c *Coordinator) Receive(ctx context.Context, node protocol.NodeRef, r protocol.Receipt) error {
	proofDone, proofErr := c.admitProof(ctx)
	if proofErr != nil {
		return proofErr
	}
	defer proofDone()
	done, admitErr := c.begin(ctx)
	if admitErr != nil {
		return admitErr
	}
	defer done()
	if e := c.checkIssuer(ctx); e != nil {
		return e
	}
	if r.Node != node || r.Type != "formatComplete" || !r.FilesystemChecked {
		return protocol.Fail(protocol.Invalid, "unsupported or unauthenticated filesystem receipt")
	}
	v, e := c.repo.FindVolume(ctx, r.Volume.Handle)
	if e != nil {
		return e
	}
	op, e := c.repo.Operation(ctx, r.JobID)
	if e != nil {
		return e
	}
	if v.Status.FormatOp != op.Name || op.Spec.Type != "format" || op.Spec.Nonce != r.Nonce || r.Volume != volumeRef(c.repo, v) || node.BindingID != v.Status.NodeBindingID {
		return protocol.Fail(protocol.Blocked, "format receipt ownership differs")
	}
	_, _, d, e := c.stageFacts(ctx, protocol.StageRequest{Node: node, Volume: r.Volume, Snapshot: r.Snapshot})
	if e != nil {
		return e
	}
	if d.FSType != "ext4" || d.FSUUID != v.Status.FSUUID {
		return protocol.Fail(protocol.RecoveryRequired, "own ext4 filesystem not proven")
	}
	v.Status.Initialization = "FormatComplete"
	if _, e = c.repo.SaveVolume(ctx, v, v.Version); e != nil {
		return e
	}
	fresh, e := c.repo.Operation(ctx, op.Name)
	if e != nil {
		return e
	}
	fresh.Status.Receipt = &r
	if _, e = c.repo.SaveOperation(ctx, fresh, fresh.Version); e != nil {
		return e
	}
	return c.finish(ctx, op, "filesystem verified")
}
