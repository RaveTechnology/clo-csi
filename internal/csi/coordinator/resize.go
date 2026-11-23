package coordinator

import (
	"context"
	"github.com/RaveTechnology/clo-cloud-go/cloud"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"math"
	"slices"
)

func (c *Coordinator) Expand(ctx context.Context, handle string, required, limit int64) (int64, error) {
	done, admitErr := c.begin(ctx)
	if admitErr != nil {
		return 0, admitErr
	}
	defer done()
	gb, _, e := capacity(required, limit)
	if e != nil || required <= 0 {
		if e == nil {
			e = protocol.Fail(protocol.Invalid, "positive expansion capacity required")
		}
		return 0, e
	}
	if e = c.checkIssuer(ctx); e != nil {
		return 0, e
	}
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	v, e := c.repo.FindVolume(ctx, handle)
	if e != nil {
		return 0, e
	}
	if v.Status.Deleted || v.Status.Retired || v.Status.Quarantined {
		return 0, protocol.Fail(protocol.Blocked, "volume unavailable")
	}
	if pending, e := c.pending(ctx, v, "extend"); pending {
		if e != nil {
			return 0, e
		}
		v, e = c.repo.Volume(ctx, v.Name)
		if e != nil {
			return 0, e
		}
	}
	a, e := c.ownedVolume(ctx, v)
	if e != nil {
		return 0, e
	}
	if a.SizeGB <= 0 || a.SizeGB > int(math.MaxInt64/GiB) {
		return 0, protocol.Fail(protocol.RecoveryRequired, "invalid actual size")
	}
	if a.SizeGB >= gb {
		if a.Status != "AVAILABLE" && a.Status != "IN_USE" {
			return 0, protocol.Fail(protocol.RecoveryRequired, "reported expansion capacity is not ready")
		}
		if limit > 0 && int64(a.SizeGB)*GiB > limit {
			return 0, protocol.Fail(protocol.Invalid, "existing size exceeds limit")
		}
		return int64(a.SizeGB) * GiB, nil
	}
	n, e := c.expansionNode(ctx, v, a)
	if e != nil {
		return 0, e
	}
	op, finishPreparation, e := c.newOperation(ctx, "extend", v, &n, protocol.Snapshot{}, nil, gb, limit)
	defer finishPreparation()
	if e != nil {
		if code(e, protocol.Conflict) {
			// A completed competing grow can invalidate our pre-claim facts.
			// Observe its newer capacity; never dispatch from the stale snapshot.
			fresh, readErr := c.repo.Volume(ctx, v.Name)
			if readErr == nil && fresh.Status.PendingOp == "" && !fresh.Status.Deleted && !fresh.Status.Retired && !fresh.Status.Quarantined {
				actual, readErr := c.ownedVolume(ctx, fresh)
				if readErr == nil && actual.SizeGB >= gb {
					if actual.Status != "AVAILABLE" && actual.Status != "IN_USE" {
						return 0, protocol.Fail(protocol.RecoveryRequired, "reported expansion capacity is not ready")
					}
					if rangeErr := actualCapacity(actual.SizeGB, limit); rangeErr != nil {
						return 0, rangeErr
					}
					return int64(actual.SizeGB) * GiB, nil
				}
			}
		}
		return 0, e
	}
	// Holding both durable claims serializes growth with driver detach. Re-read
	// provider attachment after acquiring them; a pre-claim observation can be stale.
	latest, e := c.repo.Volume(ctx, v.Name)
	if e == nil {
		a, e = c.ownedVolume(ctx, latest)
	}
	if e == nil {
		var bound registry.NodeRecord
		bound, e = c.expansionNode(ctx, latest, a)
		if e == nil && (bound.Name != n.Name || bound.Spec.Ref != n.Spec.Ref) {
			e = protocol.Fail(protocol.Blocked, "expansion attachment changed before dispatch")
		}
		if e == nil {
			e = c.expansionGuest(ctx, latest, bound)
		}
	}
	if e != nil {
		c.abortPrepared(ctx, op)
		return 0, e
	}
	if e = c.send(ctx, op, func() error { return c.api.ExtendVolume(ctx, v.Status.ProviderID, gb) }); e != nil {
		return 0, e
	}
	a, e = c.waitVolume(ctx, v, func(a cloud.Volume) bool {
		return a.SizeGB >= gb && a.Status == "IN_USE" && a.AttachedServerID == n.Spec.Ref.ServerID
	})
	if e != nil {
		c.uncertain(ctx, op, "extend postcondition unresolved")
		return 0, protocol.Fail(protocol.RecoveryRequired, "extend unresolved: %s", op.Name)
	}
	if e = actualCapacity(a.SizeGB, limit); e != nil {
		c.uncertain(ctx, op, "provider extension capacity outside saved range")
		return 0, e
	}
	if e = c.operationAuthority(ctx, op); e != nil {
		return 0, e
	}
	v, e = c.repo.Volume(ctx, v.Name)
	if e != nil {
		return 0, e
	}
	v.Status.SizeGB = a.SizeGB
	if _, e = c.repo.SaveVolume(ctx, v, v.Version); e != nil {
		return 0, e
	}
	if e = c.finish(ctx, op, "extended"); e != nil {
		return 0, e
	}
	return int64(a.SizeGB) * GiB, nil
}

func (c *Coordinator) expansionNode(ctx context.Context, v registry.VolumeRecord, a cloud.Volume) (registry.NodeRecord, error) {
	var none registry.NodeRecord
	if a.Status != "IN_USE" || a.AttachedServerID == "" || v.Status.NodeBindingID == "" ||
		!v.Status.Published || v.Status.Serial == "" || v.Status.Barrier ||
		v.Status.Deleted || v.Status.Retired || v.Status.Quarantined {
		return none, protocol.Fail(protocol.Blocked, "CLO expansion requires a volume attached to its registered node; attach it before requesting growth")
	}
	n, e := c.repo.Node(ctx, v.Status.NodeBindingID)
	if e != nil {
		return none, e
	}
	if !n.Status.Initialized || n.Status.Fenced || n.Status.Quarantined || n.Spec.Ref.ServerID != a.AttachedServerID {
		return none, protocol.Fail(protocol.Blocked, "expansion node binding unavailable or attachment differs")
	}
	server, e := c.ownedServer(ctx, n)
	if e != nil {
		return none, e
	}
	if server.Status != "ACTIVE" || server.SwitchStatus != "ON" || !slices.Contains(server.DiskIDs, a.ID) {
		return none, protocol.Fail(protocol.Blocked, "expansion requires an active registered VM with the attached volume")
	}
	return n, nil
}

func (c *Coordinator) expansionGuest(ctx context.Context, v registry.VolumeRecord, n registry.NodeRecord) error {
	r, e := c.exchange(ctx, "baseline", n.Spec.Ref, volumeRef(c.repo, v), protocol.IssuerDescriptor{}, "", "")
	if e != nil {
		return e
	}
	_, e = c.checkInventory(ctx, n, r.Snapshot, "")
	return e
}
