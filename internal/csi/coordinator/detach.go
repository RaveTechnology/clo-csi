package coordinator

import (
	"context"
	"github.com/RaveTechnology/clo-cloud-go/cloud"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
)

func (c *Coordinator) Unpublish(ctx context.Context, handle, nodeID string) error {
	done, admitErr := c.begin(ctx)
	if admitErr != nil {
		return admitErr
	}
	defer done()
	if e := c.checkIssuer(ctx); e != nil {
		return e
	}
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	v, e := c.repo.FindVolume(ctx, handle)
	if e != nil {
		return e
	}
	if nodeID != "" {
		if _, e = c.nodeIDParts(nodeID); e != nil {
			return e
		}
		if v.Status.NodeBindingID != "" {
			n, e := c.repo.Node(ctx, v.Status.NodeBindingID)
			if e != nil {
				return e
			}
			if EncodeNodeID(n.Spec.Ref) != nodeID {
				return nil
			}
		}
	}
	if pending, e := c.pending(ctx, v, "detach"); pending {
		if e != nil {
			return e
		}
		v, e = c.repo.Volume(ctx, v.Name)
		if e != nil {
			return e
		}
	}
	a, e := c.ownedVolume(ctx, v)
	if e != nil {
		if code(e, protocol.NotFound) && v.Status.Deleted {
			return nil
		}
		return e
	}
	if a.AttachedServerID == "" && v.Status.NodeBindingID == "" {
		return nil
	}
	if v.Status.NodeBindingID == "" {
		return protocol.Fail(protocol.Blocked, "actual attachment has no registered owner")
	}
	n, e := c.repo.Node(ctx, v.Status.NodeBindingID)
	if e != nil {
		return e
	}
	alreadyFree := a.AttachedServerID == "" && a.Status == "AVAILABLE" && n.Status.Fenced
	if a.AttachedServerID != n.Spec.Ref.ServerID && !alreadyFree || v.Status.Quarantined || (n.Status.Quarantined && !n.Status.Fenced) {
		return protocol.Fail(protocol.Blocked, "attachment ownership differs or quarantined")
	}
	if !v.Status.Barrier {
		v.Status.Barrier = true
		v, e = c.repo.SaveVolume(ctx, v, v.Version)
		if e != nil {
			return e
		}
	}
	hot := true
	var before protocol.Snapshot
	var ids []string
	if n.Status.Fenced {
		if e = c.checkNodeFence(ctx, n); e != nil {
			return e
		}
		hot = false
	} else {
		baseline, e := c.exchange(ctx, "baseline", n.Spec.Ref, volumeRef(c.repo, v), protocol.IssuerDescriptor{}, "", "")
		if e != nil {
			return protocol.Fail(protocol.Blocked, "BlockedAwaitFence: explicit old VM stop/delete required for binding %s", n.Name)
		}
		ids, e = c.checkInventory(ctx, n, baseline.Snapshot, "")
		if e != nil {
			return e
		}
		r, e := c.exchange(ctx, "quiesce", n.Spec.Ref, volumeRef(c.repo, v), protocol.IssuerDescriptor{}, "", "")
		if e != nil {
			return protocol.Fail(protocol.Blocked, "BlockedAwaitFence: node quiesce unavailable for %s", n.Name)
		}
		if !r.Quiesced || r.Snapshot.BootID != baseline.Snapshot.BootID {
			return protocol.Fail(protocol.Blocked, "current boot quiesce receipt required")
		}
		found := false
		for _, d := range r.Snapshot.Disks {
			if d.Serial == v.Status.Serial {
				if found || d.Root || d.Partitioned || len(d.Mounts) > 0 {
					return protocol.Fail(protocol.Blocked, "quiesce device identity or mounts differ")
				}
				found = true
			}
		}
		if !found {
			return protocol.Fail(protocol.Blocked, "quiesce serial missing")
		}
		before = r.Snapshot
	}
	op, finishPreparation, e := c.newOperation(ctx, "detach", v, &n, before, ids, 0)
	defer finishPreparation()
	if e != nil {
		return e
	}
	if !hot {
		if e = c.checkNodeFence(ctx, n); e != nil {
			c.abortPrepared(ctx, op)
			return e
		}
	}
	if alreadyFree {
		return c.completeDetach(ctx, op)
	}
	if e = c.send(ctx, op, func() error { return c.api.DetachVolume(ctx, v.Status.ProviderID, cloud.DetachOptions{Hot: hot}) }); e != nil {
		return e
	}
	if _, e = c.waitVolume(ctx, v, func(a cloud.Volume) bool { return a.Status == "AVAILABLE" && a.AttachedServerID == "" }); e != nil {
		c.uncertain(ctx, op, "detach postcondition unresolved")
		return protocol.Fail(protocol.RecoveryRequired, "detach unresolved: %s", op.Name)
	}
	return c.completeDetach(ctx, op)
}
func (c *Coordinator) nodeIDParts(id string) ([]string, error) {
	p := splitNodeID(id, c.cfg.ProjectID)
	if p == nil {
		return nil, protocol.Fail(protocol.Invalid, "invalid node ID")
	}
	return p, nil
}
func (c *Coordinator) completeDetach(ctx context.Context, op registry.OperationRecord) error {
	if e := c.operationAuthority(ctx, op); e != nil {
		return e
	}
	v, e := c.repo.Volume(ctx, op.Spec.VolumeRecordID)
	if e != nil {
		return e
	}
	a, e := c.ownedVolume(ctx, v)
	if e != nil {
		return e
	}
	if a.Status != "AVAILABLE" || a.AttachedServerID != "" {
		return protocol.Fail(protocol.RecoveryRequired, "detach result not confirmed")
	}
	v.Status.NodeBindingID = ""
	v.Status.Barrier = true
	if _, e = c.repo.SaveVolume(ctx, v, v.Version); e != nil {
		return e
	}
	return c.finish(ctx, op, "detached")
}
