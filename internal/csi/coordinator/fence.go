package coordinator

import (
	"context"
	"github.com/RaveTechnology/clo-cloud-go/cloud"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
)

func (c *Coordinator) observeFence(ctx context.Context, n registry.NodeRecord) (bool, error) {
	if e := c.project(ctx); e != nil {
		return false, e
	}
	server, e := c.ownedServer(ctx, n)
	if cloud.IsNotFound(e) {
		servers, e := c.api.ListServers(ctx, c.cfg.ProjectID)
		if e != nil {
			return false, protocol.Fail(protocol.Unavailable, "cannot verify server deletion in project")
		}
		for _, s := range servers {
			if s.ID == n.Spec.Ref.ServerID {
				return false, protocol.Fail(protocol.Blocked, "server404 differs from project inventory")
			}
		}
		vs, e := c.repo.Volumes(ctx)
		if e != nil {
			return false, e
		}
		for _, v := range vs {
			if v.Status.NodeBindingID != n.Name {
				continue
			}
			actual, e := c.ownedVolume(ctx, v)
			if e != nil {
				return false, e
			}
			if actual.AttachedServerID != "" || actual.Status != "AVAILABLE" {
				return false, protocol.Fail(protocol.Blocked, "deleted-node fence requires all owned volumes free")
			}
		}
		return true, nil
	}
	if e != nil {
		return false, e
	}
	if server.Status != "STOPPED" || server.SwitchStatus != "OFF" {
		return false, protocol.Fail(protocol.Blocked, "explicit STOPPED/OFF or verified deletion required")
	}
	return false, nil
}
func (c *Coordinator) checkNodeFence(ctx context.Context, n registry.NodeRecord) error {
	if !n.Status.Fenced || n.Status.FenceID == "" {
		return protocol.Fail(protocol.Blocked, "explicit verified fence absent")
	}
	f, e := c.repo.Fence(ctx, n.Status.FenceID)
	if e != nil {
		return e
	}
	if !f.Status.Verified || f.Spec.BindingID != n.Name || f.Spec.ServerID != n.Spec.Ref.ServerID || f.Spec.NodeUID != n.Spec.Ref.UID || f.Spec.Epoch != n.Spec.Ref.Epoch {
		return protocol.Fail(protocol.Blocked, "fence does not match exact node binding")
	}
	_, e = c.observeFence(ctx, n)
	return e
}
func (c *Coordinator) recordFence(ctx context.Context, q protocol.RecoveryRequest) (registry.OperationRecord, error) {
	var out registry.OperationRecord
	n, e := c.repo.Node(ctx, q.BindingID)
	if e != nil {
		return out, e
	}
	deleted, e := c.observeFence(ctx, n)
	if e != nil {
		return out, e
	}
	name := registry.Name("f", q.Nonce)
	if q.DryRun {
		return registry.OperationRecord{Meta: registry.Meta{Name: name}, Status: registry.OperationStatus{Phase: registry.Prepared, Result: "would record verified explicit fence"}}, nil
	}
	f, e := c.repo.Fence(ctx, name)
	if code(e, protocol.NotFound) {
		f, e = c.repo.CreateFence(ctx, registry.FenceSpec{BindingID: n.Name, ServerID: n.Spec.Ref.ServerID, NodeUID: n.Spec.Ref.UID, Epoch: n.Spec.Ref.Epoch, Operator: q.Operator, Nonce: q.Nonce})
	}
	if e != nil {
		return out, e
	}
	if f.Spec.BindingID != n.Name || f.Spec.Nonce != q.Nonce || f.Spec.Operator != q.Operator {
		return out, protocol.Fail(protocol.Conflict, "fence nonce belongs to different request")
	}
	if !f.Status.Verified {
		f.Status.Verified = true
		f.Status.Deleted = deleted
		f.Status.Issuer = c.current()
		f, e = c.repo.SaveFence(ctx, f, f.Version)
		if e != nil {
			return out, e
		}
	}
	n, e = c.repo.Node(ctx, n.Name)
	if e != nil {
		return out, e
	}
	n.Status.Fenced = true
	n.Status.FenceID = f.Name
	n.Status.RecoveryRequests, e = auditRequest(n.Status.RecoveryRequests, q)
	if e != nil {
		return out, e
	}
	if _, e = c.repo.SaveNode(ctx, n, n.Version); e != nil {
		return out, e
	}
	return registry.OperationRecord{Meta: registry.Meta{Name: f.Name}, Status: registry.OperationStatus{Phase: registry.Completed, Result: "explicit fence verified"}}, nil
}
