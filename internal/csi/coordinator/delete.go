package coordinator

import (
	"context"
	"github.com/RaveTechnology/clo-cloud-go/cloud"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
)

func (c *Coordinator) deleted(ctx context.Context, v registry.VolumeRecord) (bool, error) {
	vs, e := c.inventory(ctx)
	if e != nil {
		return false, e
	}
	for _, a := range vs {
		if a.ID == v.Status.ProviderID {
			return false, nil
		}
	}
	_, e = c.api.GetVolume(ctx, v.Status.ProviderID)
	if cloud.IsNotFound(e) {
		return true, nil
	}
	if e == nil {
		return false, protocol.Fail(protocol.Blocked, "volume outside expected project")
	}
	return false, protocol.Fail(protocol.Unavailable, "volume deletion not confirmed")
}
func (c *Coordinator) Delete(ctx context.Context, handle string) error {
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
		if code(e, protocol.NotFound) {
			id, err := c.repo.ParseHandle(handle)
			if err != nil {
				return err
			}
			if err = c.audit(ctx); err != nil {
				return err
			}
			actual, err := c.inventory(ctx)
			if err != nil {
				return err
			}
			for _, a := range actual {
				if a.ID == id {
					return protocol.Fail(protocol.Blocked, "unregistered existing volume cannot be deleted")
				}
			}
			_, err = c.api.GetVolume(ctx, id)
			if cloud.IsNotFound(err) {
				return nil
			}
			if err == nil {
				return protocol.Fail(protocol.Blocked, "unregistered volume exists outside verified project")
			}
			return protocol.Fail(protocol.Unavailable, "cannot confirm absent unregistered volume")
		}
		return e
	}
	if v.Status.Deleted {
		gone, e := c.deleted(ctx, v)
		if e != nil {
			return e
		}
		if gone {
			return nil
		}
		return protocol.Fail(protocol.Blocked, "deleted provider ID reappeared")
	}
	if pending, e := c.pending(ctx, v, "delete"); pending {
		if e != nil {
			return e
		}
		return nil
	}
	if v.Status.NodeBindingID != "" || v.Status.Quarantined || v.Status.Retired {
		return protocol.Fail(protocol.Blocked, "attached/quarantined/retired volume cannot be deleted")
	}
	a, e := c.ownedVolume(ctx, v)
	if e != nil {
		return e
	}
	if a.AttachedServerID != "" || a.Bootable || a.Status != "AVAILABLE" {
		return protocol.Fail(protocol.Blocked, "only available unbootable unattached volumes can be deleted")
	}
	op, finishPreparation, e := c.newOperation(ctx, "delete", v, nil, protocol.Snapshot{}, nil, 0)
	defer finishPreparation()
	if e != nil {
		return e
	}
	if e = c.send(ctx, op, func() error { return c.api.DeleteVolume(ctx, v.Status.ProviderID) }); e != nil {
		return e
	}
	return c.completeDelete(ctx, op)
}
func (c *Coordinator) completeDelete(ctx context.Context, op registry.OperationRecord) error {
	if e := c.operationAuthority(ctx, op); e != nil {
		return e
	}
	v, e := c.repo.Volume(ctx, op.Spec.VolumeRecordID)
	if e != nil {
		return e
	}
	e = cloud.Wait(ctx, c.cfg.PollInterval, func(ctx context.Context) (string, error) {
		gone, err := c.deleted(ctx, v)
		if err != nil {
			return "", err
		}
		if gone {
			return "GONE", nil
		}
		return "PENDING", nil
	}, func(s string) bool { return s == "GONE" })
	if e != nil {
		c.uncertain(ctx, op, "delete postcondition unresolved")
		return protocol.Fail(protocol.RecoveryRequired, "deletion remains pending: %s", op.Name)
	}
	v.Status.Deleted = true
	if _, e = c.repo.SaveVolume(ctx, v, v.Version); e != nil {
		return e
	}
	return c.finish(ctx, op, "deleted")
}
