package coordinator

import (
	"context"
	"github.com/RaveTechnology/clo-cloud-go/cloud"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"github.com/google/uuid"
	"strings"
)

// Maintain records the operator's intent before changing maintenance state.
// It uses the same coordinator as CSI; it never dispatches a native cloud write.
func (c *Coordinator) Maintain(ctx context.Context, q protocol.RecoveryRequest, blob []byte) ([]byte, error) {
	if strings.TrimSpace(q.Operator) == "" || len(q.Operator) > 128 {
		return nil, protocol.Fail(protocol.Invalid, "explicit operator required")
	}
	if _, e := uuid.Parse(q.Nonce); e != nil {
		return nil, protocol.Fail(protocol.Invalid, "maintenance UUID nonce required")
	}
	if q.Action != "freeze" && q.Action != "resume" && q.Action != "restore" && q.Action != "export" {
		return nil, protocol.Fail(protocol.Invalid, "unknown maintenance action")
	}
	if q.DryRun {
		return nil, nil
	}
	if q.Action == "export" {
		return c.Export(ctx)
	}
	if e := c.repo.RecordMaintenance(ctx, q); e != nil {
		return nil, e
	}
	switch q.Action {
	case "freeze":
		return nil, c.Freeze(ctx)
	case "resume":
		return nil, c.Resume(ctx)
	default:
		return nil, c.Restore(ctx, blob)
	}
}

func (c *Coordinator) begin(ctx context.Context) (func(), error) {
	if e := c.repo.RecoverInitializations(ctx); e != nil {
		return nil, e
	}
	f, e := c.repo.Frozen(ctx)
	if e != nil && !code(e, protocol.NotFound) {
		return nil, e
	}
	if f {
		return nil, protocol.Fail(protocol.Blocked, "registry writers frozen")
	}
	c.workMu.Lock()
	defer c.workMu.Unlock()
	if c.stopping {
		return nil, protocol.Fail(protocol.Blocked, "registry writers draining")
	}
	if c.active == 0 {
		c.drained = make(chan struct{})
	}
	c.active++
	return func() {
		c.workMu.Lock()
		defer c.workMu.Unlock()
		c.active--
		if c.active == 0 {
			close(c.drained)
		}
	}, nil
}
func (c *Coordinator) Freeze(ctx context.Context) error {
	c.workMu.Lock()
	c.stopping = true
	done := c.drained
	if c.active == 0 {
		done = nil
	}
	c.workMu.Unlock()
	if e := c.repo.SetFrozen(ctx, true); e != nil {
		return e
	}
	issuer, e := c.repo.Issuer(ctx)
	if e != nil {
		return e
	}
	if issuer.Status.Current.Valid() && issuer.Status.Current != c.current() && !issuer.Status.RestoreNeedsFence {
		c.mu.RLock()
		pod, nonce := c.launchPodUID, c.launchNonce
		c.mu.RUnlock()
		if pod == "" || nonce == "" {
			return protocol.Fail(protocol.Blocked, "cannot drain a different live issuer without host termination proof")
		}
		if e = c.establishIssuer(ctx, pod, nonce); e != nil {
			return e
		}
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
func (c *Coordinator) Resume(ctx context.Context) error {
	issuer, e := c.repo.Issuer(ctx)
	if e != nil {
		return e
	}
	if issuer.Status.RestoreNeedsFence {
		if e = c.recoverLostEpoch(ctx); e != nil {
			return e
		}
		issuer, e = c.repo.Issuer(ctx)
		if e != nil {
			return e
		}
	}
	if !issuer.Status.Current.Valid() || issuer.Status.Current != c.current() {
		c.mu.RLock()
		pod, nonce := c.launchPodUID, c.launchNonce
		c.mu.RUnlock()
		if pod == "" || nonce == "" {
			return protocol.Fail(protocol.Blocked, "verified current controller identity required before resume")
		}
		if e = c.establishIssuer(ctx, pod, nonce); e != nil {
			return e
		}
	}
	if e := c.repo.SetFrozen(ctx, false); e != nil {
		return e
	}
	c.workMu.Lock()
	c.stopping = false
	c.workMu.Unlock()
	return nil
}
func (c *Coordinator) maintenanceReady(ctx context.Context) error {
	f, e := c.repo.Frozen(ctx)
	if e != nil {
		return e
	}
	c.workMu.Lock()
	defer c.workMu.Unlock()
	if !f || !c.stopping || c.active != 0 {
		return protocol.Fail(protocol.Blocked, "freeze and drain writers before registry maintenance")
	}
	return nil
}
func (c *Coordinator) Export(ctx context.Context) ([]byte, error) {
	if e := c.maintenanceReady(ctx); e != nil {
		return nil, e
	}
	issuer, e := c.repo.Issuer(ctx)
	if e != nil {
		return nil, e
	}
	if issuer.Status.RestoreNeedsFence || !issuer.Status.Current.Valid() || issuer.Status.Current != c.current() {
		return nil, protocol.Fail(protocol.Blocked, "latest issuer must be verified and drained before export")
	}
	return c.repo.Export(ctx)
}
func (c *Coordinator) Restore(ctx context.Context, blob []byte) error {
	if e := c.maintenanceReady(ctx); e != nil {
		return e
	}
	staged := registry.New(registry.NewMemoryBackend(), c.repo.Installation())
	if e := staged.SetFrozen(ctx, true); e != nil {
		return e
	}
	if e := staged.Restore(ctx, blob); e != nil {
		return e
	}
	actual, e := c.inventory(ctx)
	if e != nil {
		return e
	}
	owned := make([]registry.ProviderVolume, 0, len(actual))
	for _, a := range actual {
		owned = append(owned, registry.ProviderVolume{ID: a.ID, Name: a.Name})
	}
	if e = staged.AuditOwned(ctx, owned); e != nil {
		return e
	}
	volumes, e := staged.Volumes(ctx)
	if e != nil {
		return e
	}
	for _, v := range volumes {
		if v.Status.ProviderID == "" {
			continue
		}
		found := false
		for _, a := range actual {
			if a.ID != v.Status.ProviderID {
				continue
			}
			if a.Name != v.Spec.ProviderName || a.Bootable || v.Status.Deleted {
				return protocol.Fail(protocol.Blocked, "backup provider identity differs from project")
			}
			found = true
		}
		if !found {
			if !v.Status.Deleted {
				return protocol.Fail(protocol.Blocked, "backup owned volume absent from project")
			}
			gone, e := c.deleted(ctx, v)
			if e != nil {
				return e
			}
			if !gone {
				return protocol.Fail(protocol.Blocked, "backup tombstone provider still exists")
			}
		}
	}
	nodes, e := staged.Nodes(ctx)
	if e != nil {
		return e
	}
	for _, n := range nodes {
		server, e := c.api.GetServer(ctx, n.Spec.Ref.ServerID)
		if cloud.IsNotFound(e) {
			servers, e := c.api.ListServers(ctx, c.cfg.ProjectID)
			if e != nil {
				return protocol.Fail(protocol.Unavailable, "restore server inventory unavailable")
			}
			for _, s := range servers {
				if s.ID == n.Spec.Ref.ServerID {
					return protocol.Fail(protocol.Blocked, "restore404 differs from project inventory")
				}
			}
			continue
		}
		if e != nil {
			return protocol.Fail(protocol.Unavailable, "restore server detail unavailable")
		}
		if server.ID != n.Spec.Ref.ServerID || server.ProjectID != n.Spec.ProjectID || server.Name != n.Spec.Identity.ProviderName {
			return protocol.Fail(protocol.Blocked, "backup server ownership differs")
		}
	}
	return c.repo.Restore(ctx, blob)
}
