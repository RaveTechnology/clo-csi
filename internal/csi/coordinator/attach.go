package coordinator

import (
	"context"
	"github.com/RaveTechnology/clo-cloud-go/cloud"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
)

func sameBaseline(a, b protocol.Snapshot) bool {
	if a.BootID != b.BootID {
		return false
	}
	ar, ae := rootSerial(a)
	br, be := rootSerial(b)
	if ae != nil || be != nil || ar != br {
		return false
	}
	var as, bs []string
	for _, d := range a.Disks {
		as = append(as, d.Serial)
	}
	for _, d := range b.Disks {
		bs = append(bs, d.Serial)
	}
	return sameIDs(as, bs)
}
func (c *Coordinator) quarantine(ctx context.Context, v registry.VolumeRecord, n registry.NodeRecord, reason string) {
	if fresh, e := c.repo.Volume(ctx, v.Name); e == nil {
		fresh.Status.Quarantined = true
		fresh.Status.Reason = reason
		_, _ = c.repo.SaveVolume(ctx, fresh, fresh.Version)
	}
	if fresh, e := c.repo.Node(ctx, n.Name); e == nil {
		fresh.Status.Quarantined = true
		fresh.Status.Reason = reason
		_, _ = c.repo.SaveNode(ctx, fresh, fresh.Version)
	}
}
func (c *Coordinator) Publish(ctx context.Context, handle, nodeID string, readonly bool) (protocol.PublishContext, error) {
	done, admitErr := c.begin(ctx)
	if admitErr != nil {
		return protocol.PublishContext{}, admitErr
	}
	defer done()
	var out protocol.PublishContext
	if readonly {
		return out, protocol.Fail(protocol.Invalid, "controller read-only attachment is unsupported")
	}
	if e := c.checkIssuer(ctx); e != nil {
		return out, e
	}
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	v, e := c.repo.FindVolume(ctx, handle)
	if e != nil {
		return out, e
	}
	n, e := c.node(ctx, nodeID)
	if e != nil {
		return out, e
	}
	if v.Status.Deleted || v.Status.Retired || v.Status.Quarantined || n.Status.Fenced || n.Status.Quarantined {
		return out, protocol.Fail(protocol.Blocked, "volume or node unavailable/quarantined/fenced")
	}
	if pending, e := c.pending(ctx, v, "attach"); pending {
		if e != nil {
			return out, e
		}
		v, e = c.repo.Volume(ctx, v.Name)
		if e != nil {
			return out, e
		}
	}
	a, e := c.ownedVolume(ctx, v)
	if e != nil {
		return out, e
	}
	if a.AttachedServerID != "" && a.AttachedServerID != n.Spec.Ref.ServerID {
		return out, protocol.Fail(protocol.Blocked, "volume attached to another server")
	}
	if a.AttachedServerID == n.Spec.Ref.ServerID {
		if v.Status.NodeBindingID != n.Name || v.Status.Serial == "" || v.Status.Barrier {
			return out, protocol.Fail(protocol.Blocked, "attachment does not match durable serial generation")
		}
		return protocol.PublishContext{Volume: volumeRef(c.repo, v), Node: n.Spec.Ref}, nil
	}
	if n.Status.PendingEnrollment != "" {
		return out, protocol.Fail(protocol.Conflict, "node enrollment is busy")
	}
	if v.Status.PendingOp != "" {
		return out, protocol.Fail(protocol.Conflict, "volume is busy")
	}
	baseline, e := c.exchange(ctx, "baseline", n.Spec.Ref, volumeRef(c.repo, v), protocol.IssuerDescriptor{}, "", "")
	if e != nil {
		return out, e
	}
	ids, e := c.checkInventory(ctx, n, baseline.Snapshot, "")
	if e != nil {
		if code(e, protocol.Blocked) {
			c.quarantine(ctx, v, n, "unexpected node baseline")
		}
		return out, e
	}
	if len(ids)-len(n.Status.BootVolumeIDs) >= c.cfg.MaxVolumesPerNode {
		return out, protocol.Fail(protocol.Blocked, "node volume limit reached")
	}
	op, e := c.newOperation(ctx, "attach", v, &n, baseline.Snapshot, ids, 0)
	if e != nil {
		return out, e
	}
	again, e := c.exchange(ctx, "baseline", n.Spec.Ref, volumeRef(c.repo, v), protocol.IssuerDescriptor{}, "", "")
	if e != nil {
		c.abortPrepared(ctx, op)
		return out, e
	}
	currentIDs, e := c.checkInventory(ctx, n, again.Snapshot, "")
	if e != nil || !sameIDs(ids, currentIDs) || !sameBaseline(baseline.Snapshot, again.Snapshot) {
		c.abortPrepared(ctx, op)
		return out, protocol.Fail(protocol.Conflict, "baseline changed before dispatch; acquire a new baseline")
	}
	v, e = c.repo.Volume(ctx, v.Name)
	if e != nil {
		return out, e
	}
	v.Status.AttachEpoch++
	v.Status.NodeBindingID = n.Name
	v.Status.Barrier = false
	v, e = c.repo.SaveVolume(ctx, v, v.Version)
	if e != nil {
		return out, e
	}
	e = c.send(ctx, op, func() error { return c.api.AttachVolume(ctx, v.Status.ProviderID, n.Spec.Ref.ServerID) })
	if e != nil {
		return out, e
	}
	if _, e = c.waitVolume(ctx, v, func(a cloud.Volume) bool { return a.Status == "IN_USE" && a.AttachedServerID == n.Spec.Ref.ServerID }); e != nil {
		c.uncertain(ctx, op, "attach postcondition unresolved")
		return out, protocol.Fail(protocol.RecoveryRequired, "attach unresolved: %s", op.Name)
	}
	if e = c.completeEnrollment(ctx, op); e != nil {
		c.uncertain(ctx, op, "serial enrollment unresolved")
		return out, e
	}
	v, e = c.repo.Volume(ctx, v.Name)
	if e != nil {
		return out, e
	}
	return protocol.PublishContext{Volume: volumeRef(c.repo, v), Node: n.Spec.Ref}, nil
}
func (c *Coordinator) observeEnrollment(ctx context.Context, op registry.OperationRecord) (registry.VolumeRecord, registry.NodeRecord, string, error) {
	v, e := c.repo.Volume(ctx, op.Spec.VolumeRecordID)
	if e != nil {
		return v, registry.NodeRecord{}, "", e
	}
	n, e := c.repo.Node(ctx, op.Spec.NodeBindingID)
	if e != nil {
		return v, n, "", e
	}
	a, e := c.ownedVolume(ctx, v)
	if e != nil {
		return v, n, "", e
	}
	if a.Status != "IN_USE" || a.AttachedServerID != n.Spec.Ref.ServerID {
		return v, n, "", protocol.Fail(protocol.RecoveryRequired, "intended attachment not terminal on exact server")
	}
	r, e := c.exchange(ctx, "observe", n.Spec.Ref, volumeRef(c.repo, v), protocol.IssuerDescriptor{}, "", "")
	if e != nil {
		return v, n, "", e
	}
	server, e := c.ownedServer(ctx, n)
	if e != nil {
		return v, n, "", e
	}
	expected := append(append([]string(nil), op.Spec.APIVolumeIDs...), v.Status.ProviderID)
	if !sameIDs(expected, server.DiskIDs) {
		return v, n, "", protocol.Fail(protocol.Blocked, "post-attach API delta differs")
	}
	if _, e = c.checkInventory(ctx, n, r.Snapshot, v.Status.ProviderID); e != nil {
		return v, n, "", e
	}
	beforeRoot, e := rootSerial(op.Spec.Before)
	if e != nil {
		return v, n, "", e
	}
	afterRoot, e := rootSerial(r.Snapshot)
	if e != nil || beforeRoot != afterRoot || afterRoot != n.Status.RootSerial {
		return v, n, "", protocol.Fail(protocol.Blocked, "post-attach root differs")
	}
	before := map[string]bool{}
	for _, d := range op.Spec.Before.Disks {
		before[d.Serial] = true
	}
	after := map[string]bool{}
	delta := ""
	for _, d := range r.Snapshot.Disks {
		after[d.Serial] = true
		if !before[d.Serial] {
			if delta != "" || d.Root || d.Partitioned {
				return v, n, "", protocol.Fail(protocol.Blocked, "serial delta ambiguous")
			}
			delta = d.Serial
		}
	}
	for serial := range before {
		if !after[serial] {
			return v, n, "", protocol.Fail(protocol.Blocked, "previous serial disappeared")
		}
	}
	if delta == "" || v.Status.Serial != "" && v.Status.Serial != delta {
		return v, n, "", protocol.Fail(protocol.Blocked, "serial binding differs")
	}
	return v, n, delta, nil
}
func (c *Coordinator) completeEnrollment(ctx context.Context, op registry.OperationRecord) error {
	if e := c.operationAuthority(ctx, op); e != nil {
		return e
	}
	v, n, serial, e := c.observeEnrollment(ctx, op)
	if e != nil {
		if code(e, protocol.Blocked) && v.Name != "" && n.Name != "" {
			c.quarantine(ctx, v, n, "ambiguous attachment/serial delta")
		}
		return e
	}
	v.Status.Serial = serial
	v.Status.NodeBindingID = n.Name
	v.Status.Published = true
	if _, e = c.repo.SaveVolume(ctx, v, v.Version); e != nil {
		return e
	}
	return c.finish(ctx, op, "serial bound")
}
