package coordinator

import (
	"context"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
)

// Already-admitted work may finalize while freeze drains it, but a recreated
// operation or replaced issuer can never change freshly restored metadata.
func (c *Coordinator) operationIdentity(ctx context.Context, op registry.OperationRecord) (registry.OperationRecord, error) {
	latest, e := c.repo.Operation(ctx, op.Name)
	if e != nil {
		return latest, e
	}
	if latest.UID != op.UID || latest.Spec.Nonce != op.Spec.Nonce || latest.Spec.Issuer != op.Spec.Issuer {
		return latest, protocol.Fail(protocol.Conflict, "stale operation object/incarnation")
	}
	issuer, e := c.repo.Issuer(ctx)
	if e != nil {
		return latest, e
	}
	if issuer.Status.Current != c.current() || issuer.Status.RestoreNeedsFence {
		return latest, protocol.Fail(protocol.Blocked, "operation completion issuer inactive")
	}
	if e = c.priorIssuerGone(ctx, op); e != nil {
		return latest, e
	}
	return latest, nil
}

func (c *Coordinator) operationAuthority(ctx context.Context, op registry.OperationRecord) error {
	latest, e := c.operationIdentity(ctx, op)
	if e != nil {
		return e
	}
	if latest.Status.Phase != registry.Completed {
		if op.Spec.VolumeRecordID != "" {
			v, e := c.repo.Volume(ctx, op.Spec.VolumeRecordID)
			if e != nil {
				return e
			}
			if v.Status.PendingOp != op.Name {
				return protocol.Fail(protocol.Conflict, "operation lost volume claim")
			}
		}
		if op.Spec.NodeBindingID != "" {
			n, e := c.repo.Node(ctx, op.Spec.NodeBindingID)
			if e != nil {
				return e
			}
			if n.Status.PendingEnrollment != op.Name {
				return protocol.Fail(protocol.Conflict, "operation lost node claim")
			}
		}
	}
	return nil
}
