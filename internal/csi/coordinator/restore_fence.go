package coordinator

import (
	"context"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"strings"
)

// Losing the live epoch also loses knowledge of newer paused processes. This
// conservative disaster path requires hardware fences, never a backup's TTL or
// historical PID proof. It does not resolve or resend any pending cloud job.
func (c *Coordinator) recoverLostEpoch(ctx context.Context) error {
	if e := c.maintenanceReady(ctx); e != nil {
		return e
	}
	self, e := c.self(ctx)
	if e != nil {
		return e
	}
	host, e := c.repo.Node(ctx, self.BindingID)
	if e != nil {
		return e
	}
	if e = c.project(ctx); e != nil {
		return e
	}
	servers, e := c.api.ListServers(ctx, c.cfg.ProjectID)
	if e != nil {
		return protocol.Fail(protocol.Unavailable, "disaster recovery server inventory unavailable")
	}
	foundSelf := false
	seen := map[string]bool{}
	for _, listed := range servers {
		if listed.ID == "" || seen[listed.ID] || listed.ProjectID != c.cfg.ProjectID {
			return protocol.Fail(protocol.Blocked, "disaster recovery project inventory differs")
		}
		seen[listed.ID] = true
		actual, e := c.api.GetServer(ctx, listed.ID)
		if e != nil {
			return protocol.Fail(protocol.Unavailable, "disaster recovery server state not confirmed")
		}
		if actual.ID != listed.ID || actual.ProjectID != c.cfg.ProjectID || actual.Name != listed.Name {
			return protocol.Fail(protocol.Blocked, "disaster recovery VM identity differs")
		}
		if actual.ID == self.ServerID {
			if actual.Name != host.Spec.Identity.ProviderName || actual.Status != "ACTIVE" || actual.SwitchStatus != "ON" {
				return protocol.Fail(protocol.Blocked, "maintenance VM must be the verified active controller host")
			}
			foundSelf = true
			continue
		}
		if actual.Status != "STOPPED" || actual.SwitchStatus != "OFF" {
			return protocol.Fail(protocol.Blocked, "lost epoch: explicitly stop other project VM %s before recovery; CSI will not stop it", actual.ID)
		}
	}
	if !foundSelf {
		return protocol.Fail(protocol.Blocked, "maintenance host absent from expected project")
	}
	r, e := c.exchange(ctx, "listIssuers", host.Spec.Ref, protocol.VolumeRef{}, protocol.IssuerDescriptor{}, "", "")
	if e != nil {
		return e
	}
	if len(r.Issuers) != 1 || r.Issuers[0] != self {
		return protocol.Fail(protocol.Blocked, "maintenance host has another or unknown controller process")
	}
	actualVolumes, e := c.inventory(ctx)
	if e != nil {
		return e
	}
	for _, v := range actualVolumes {
		if strings.HasPrefix(v.Name, c.repo.Prefix()) && v.Status != "AVAILABLE" && v.Status != "IN_USE" {
			return protocol.Fail(protocol.RecoveryRequired, "provider volume transition remains unresolved")
		}
	}
	if e = c.audit(ctx); e != nil {
		return e
	}
	if e = c.project(ctx); e != nil {
		return e
	}
	epoch, e := c.repo.Issuer(ctx)
	if e != nil {
		return e
	}
	if !epoch.Status.Frozen || !epoch.Status.RestoreNeedsFence {
		return protocol.Fail(protocol.Conflict, "restore recovery state changed")
	}
	old := epoch.Status.Current
	if old.Valid() && old != self {
		epoch.Status.Prior = append(epoch.Status.Prior, protocol.IssuerProof{Descriptor: old, Verdict: protocol.Gone, Reason: "explicit project VM fences and exclusive authenticated maintenance host process scan"})
	}
	epoch.Status.Current = self
	epoch.Status.RestoreNeedsFence = false
	if _, e = c.repo.SaveIssuer(ctx, epoch, epoch.Version); e != nil {
		return e
	}
	c.mu.Lock()
	c.issuer = self
	c.ready = true
	c.mu.Unlock()
	return nil
}
