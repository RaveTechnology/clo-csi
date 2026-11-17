package coordinator

import (
	"context"
	"github.com/RaveTechnology/clo-cloud-go/cloud"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"github.com/google/uuid"
	"strings"
)

func EncodeNodeID(n protocol.NodeRef) string {
	return "clo-node:" + n.ProjectID + ":" + n.ServerID + ":" + n.UID
}
func splitNodeID(id, project string) []string {
	p := strings.Split(id, ":")
	if len(p) != 4 || p[0] != "clo-node" || p[1] != project {
		return nil
	}
	for _, s := range p[1:] {
		if _, e := uuid.Parse(s); e != nil {
			return nil
		}
	}
	return p
}
func (c *Coordinator) node(ctx context.Context, id string) (registry.NodeRecord, error) {
	p := strings.Split(id, ":")
	if len(p) != 4 || p[0] != "clo-node" || p[1] != c.cfg.ProjectID {
		return registry.NodeRecord{}, protocol.Fail(protocol.Invalid, "invalid node ID")
	}
	for _, v := range p[1:] {
		if _, e := uuid.Parse(v); e != nil {
			return registry.NodeRecord{}, protocol.Fail(protocol.Invalid, "invalid node UUID")
		}
	}
	ns, e := c.repo.Nodes(ctx)
	if e != nil {
		return registry.NodeRecord{}, e
	}
	for _, n := range ns {
		if n.Spec.Ref.ProjectID == p[1] && n.Spec.Ref.ServerID == p[2] && n.Spec.Ref.UID == p[3] {
			return n, nil
		}
	}
	return registry.NodeRecord{}, protocol.Fail(protocol.NotFound, "node binding absent")
}
func (c *Coordinator) Binding(ctx context.Context, caller protocol.Caller) (protocol.NodeRef, error) {
	ns, e := c.repo.Nodes(ctx)
	if e != nil {
		return protocol.NodeRef{}, e
	}
	for _, n := range ns {
		if n.Spec.Ref.UID == caller.NodeUID && n.Spec.Ref.Name == caller.NodeName {
			return n.Spec.Ref, nil
		}
	}
	return protocol.NodeRef{}, protocol.Fail(protocol.NotFound, "node not registered")
}
func rootSerial(s protocol.Snapshot) (string, error) {
	if s.BootID == "" {
		return "", protocol.Fail(protocol.Blocked, "missing boot ID")
	}
	root := ""
	seen := map[string]bool{}
	for _, d := range s.Disks {
		if d.Serial == "" || seen[d.Serial] {
			return "", protocol.Fail(protocol.Blocked, "empty/duplicate serial")
		}
		seen[d.Serial] = true
		if d.Root {
			if root != "" {
				return "", protocol.Fail(protocol.Blocked, "multiple root disks")
			}
			root = d.Serial
		}
	}
	if root == "" {
		return "", protocol.Fail(protocol.Blocked, "root ancestry missing")
	}
	return root, nil
}
func (c *Coordinator) RegisterNode(ctx context.Context, caller protocol.Caller, hello protocol.NodeHello) (protocol.NodeRef, error) {
	done, e := c.begin(ctx)
	if e != nil {
		f, check := c.repo.Frozen(ctx)
		if check == nil && f {
			return c.registerNode(ctx, caller, hello, true)
		}
		return protocol.NodeRef{}, e
	}
	defer done()
	return c.registerNode(ctx, caller, hello, false)
}
func (c *Coordinator) registerNode(ctx context.Context, caller protocol.Caller, hello protocol.NodeHello, readonly bool) (protocol.NodeRef, error) {
	if caller.NodeName == "" || caller.NodeUID == "" || caller.PodUID == "" || hello.Identity.Validate() != nil || hello.Identity.ProjectID != c.cfg.ProjectID || !strings.HasSuffix(hello.Identity.ProviderName, "-"+hello.Identity.Attempt) {
		return protocol.NodeRef{}, protocol.Fail(protocol.Invalid, "node identity/caller invalid")
	}
	if e := c.project(ctx); e != nil {
		return protocol.NodeRef{}, e
	}
	servers, e := c.api.ListServers(ctx, c.cfg.ProjectID)
	if e != nil {
		return protocol.NodeRef{}, protocol.Fail(protocol.Unavailable, "server inventory unavailable")
	}
	var found *cloud.Server
	for _, s := range servers {
		if s.ProjectID == c.cfg.ProjectID && s.Name == hello.Identity.ProviderName {
			if found != nil {
				return protocol.NodeRef{}, protocol.Fail(protocol.Blocked, "machine name not unique")
			}
			copy := s
			found = &copy
		}
	}
	if found == nil || found.Status != "ACTIVE" {
		return protocol.NodeRef{}, protocol.Fail(protocol.Blocked, "exact active machine not found")
	}
	server, e := c.api.GetServer(ctx, found.ID)
	if e != nil || server.ID != found.ID || server.ProjectID != c.cfg.ProjectID || server.Name != hello.Identity.ProviderName || server.Status != "ACTIVE" {
		return protocol.NodeRef{}, protocol.Fail(protocol.Blocked, "machine detail differs")
	}
	privateIP := ""
	for _, id := range server.AddressIDs {
		a, e := c.api.GetAddress(ctx, id)
		if e != nil {
			return protocol.NodeRef{}, protocol.Fail(protocol.Unavailable, "machine address unavailable")
		}
		if a.External || a.AttachedServerID != server.ID {
			continue
		}
		for _, ip := range caller.InternalIPs {
			if ip == a.IP {
				privateIP = ip
			}
		}
	}
	if privateIP == "" {
		return protocol.NodeRef{}, protocol.Fail(protocol.Blocked, "machine private IP differs from authenticated Node")
	}
	root, e := rootSerial(hello.Snapshot)
	if e != nil {
		return protocol.NodeRef{}, e
	}
	existing, e := c.repo.Nodes(ctx)
	if e != nil {
		return protocol.NodeRef{}, e
	}
	for _, n := range existing {
		if n.Spec.Ref.ServerID == server.ID {
			if n.Spec.Ref.UID != caller.NodeUID || n.Spec.Ref.Name != caller.NodeName || n.Spec.Identity != hello.Identity {
				return protocol.NodeRef{}, protocol.Fail(protocol.Blocked, "machine already bound to a different Node incarnation")
			}
			if n.Status.RootSerial != root || n.Status.Fenced {
				return protocol.NodeRef{}, protocol.Fail(protocol.Blocked, "root identity changed or node fenced")
			}
			if _, e = c.checkInventory(ctx, n, hello.Snapshot, ""); e != nil {
				pending, verifyErr := c.pendingAgentBinding(ctx, n, hello.Snapshot)
				if verifyErr != nil {
					return protocol.NodeRef{}, verifyErr
				}
				if !pending {
					return protocol.NodeRef{}, e
				}
			}
			if readonly {
				return n.Spec.Ref, nil
			}
			n.Status.BootID = hello.Snapshot.BootID
			if _, e = c.repo.SaveNode(ctx, n, n.Version); e != nil {
				return protocol.NodeRef{}, e
			}
			return n.Spec.Ref, nil
		}
	}
	if readonly {
		return protocol.NodeRef{}, protocol.Fail(protocol.Blocked, "frozen registry allows only verification of an existing node binding")
	}
	if len(server.DiskIDs) != 1 || len(hello.Snapshot.Disks) != 1 {
		return protocol.NodeRef{}, protocol.Fail(protocol.Blocked, "initial binding requires boot-only inventory")
	}
	vs, e := c.inventory(ctx)
	if e != nil {
		return protocol.NodeRef{}, e
	}
	bootOK := false
	for _, v := range vs {
		if v.ID == server.DiskIDs[0] && v.Bootable && v.AttachedServerID == server.ID {
			bootOK = true
		}
	}
	if !bootOK {
		return protocol.NodeRef{}, protocol.Fail(protocol.Blocked, "boot volume ownership unverified")
	}
	n, e := c.repo.CreateNodeWithStatus(ctx, registry.NodeSpec{Ref: protocol.NodeRef{Name: caller.NodeName, UID: caller.NodeUID, ProjectID: c.cfg.ProjectID, ServerID: server.ID, Epoch: 1}, Identity: hello.Identity, PrivateIP: privateIP}, registry.NodeStatus{Initialized: true, RootSerial: root, BootID: hello.Snapshot.BootID, BootVolumeIDs: server.DiskIDs})
	if e != nil {
		return protocol.NodeRef{}, e
	}

	return n.Spec.Ref, nil
}

// An existing, exact VM/root binding must keep its authenticated recovery
// channel while a saved attach awaits serial enrollment. This returns no new
// serial or permission; the durable node/volume locks remain asserted.
func (c *Coordinator) pendingAgentBinding(ctx context.Context, n registry.NodeRecord, snapshot protocol.Snapshot) (bool, error) {
	if n.Status.PendingEnrollment == "" {
		return false, nil
	}
	op, e := c.repo.Operation(ctx, n.Status.PendingEnrollment)
	if e != nil {
		return false, e
	}
	if op.Spec.Type != "attach" || op.Spec.NodeBindingID != n.Name || !op.Status.Sent || op.Status.Phase == registry.Completed {
		return false, nil
	}
	beforeRoot, e := rootSerial(op.Spec.Before)
	if e != nil {
		return false, e
	}
	root, e := rootSerial(snapshot)
	if e != nil {
		return false, e
	}
	if beforeRoot != n.Status.RootSerial || root != beforeRoot {
		return false, nil
	}
	v, e := c.repo.Volume(ctx, op.Spec.VolumeRecordID)
	if e != nil {
		return false, e
	}
	if v.Status.PendingOp != op.Name || v.Status.NodeBindingID != n.Name || v.Status.ProviderID == "" {
		return false, nil
	}
	a, e := c.ownedVolume(ctx, v)
	if e != nil {
		return false, e
	}
	if a.Status != "IN_USE" || a.AttachedServerID != n.Spec.Ref.ServerID {
		return false, nil
	}
	server, e := c.ownedServer(ctx, n)
	if e != nil {
		return false, e
	}
	expected := append(append([]string(nil), op.Spec.APIVolumeIDs...), v.Status.ProviderID)
	return sameIDs(expected, server.DiskIDs), nil
}
func (c *Coordinator) ownedServer(ctx context.Context, n registry.NodeRecord) (cloud.Server, error) {
	if e := c.project(ctx); e != nil {
		return cloud.Server{}, e
	}
	s, e := c.api.GetServer(ctx, n.Spec.Ref.ServerID)
	if e != nil {
		return s, e
	}
	if s.ID != n.Spec.Ref.ServerID || s.ProjectID != n.Spec.ProjectID || s.Name != n.Spec.Identity.ProviderName {
		return s, protocol.Fail(protocol.Blocked, "server ownership differs")
	}
	return s, nil
}

// checkInventory compares complete API and guest inventories; ignore allows only
// the intended, still-unenrolled attachment during postcondition reconciliation.
func (c *Coordinator) checkInventory(ctx context.Context, n registry.NodeRecord, snap protocol.Snapshot, ignore string) ([]string, error) {
	root, e := rootSerial(snap)
	if e != nil {
		return nil, e
	}
	if root != n.Status.RootSerial {
		return nil, protocol.Fail(protocol.Blocked, "root serial differs")
	}
	server, e := c.ownedServer(ctx, n)
	if e != nil {
		return nil, e
	}
	actual, e := c.inventory(ctx)
	if e != nil {
		return nil, e
	}
	registered, e := c.repo.Volumes(ctx)
	if e != nil {
		return nil, e
	}
	expectedIDs := append([]string(nil), n.Status.BootVolumeIDs...)
	expectedSerials := map[string]bool{root: true}
	for _, a := range actual {
		if a.AttachedServerID != server.ID || a.Bootable || a.ID == ignore {
			continue
		}
		known := false
		for _, v := range registered {
			if v.Status.ProviderID == a.ID && v.Spec.ProviderName == a.Name && v.Status.NodeBindingID == n.Name && v.Status.Serial != "" && !v.Status.Deleted {
				expectedIDs = append(expectedIDs, a.ID)
				expectedSerials[v.Status.Serial] = true
				known = true
			}
		}
		if !known {
			return nil, protocol.Fail(protocol.Blocked, "unknown API attachment on node")
		}
	}
	if ignore != "" {
		expectedIDs = append(expectedIDs, ignore)
	}
	if !sameIDs(expectedIDs, server.DiskIDs) {
		return nil, protocol.Fail(protocol.Blocked, "API disk inventory differs")
	}
	unknown := 0
	for _, d := range snap.Disks {
		if !expectedSerials[d.Serial] {
			unknown++
			if ignore == "" || d.Root || d.Partitioned {
				return nil, protocol.Fail(protocol.Blocked, "unknown guest disk")
			}
		}
	}
	if len(snap.Disks) != len(expectedSerials)+unknown {
		return nil, protocol.Fail(protocol.Blocked, "guest disk inventory differs")
	}
	for serial := range expectedSerials {
		present := false
		for _, d := range snap.Disks {
			present = present || d.Serial == serial
		}
		if !present {
			return nil, protocol.Fail(protocol.Blocked, "known serial disappeared")
		}
	}
	if ignore != "" && unknown != 1 {
		return nil, protocol.Fail(protocol.Blocked, "intended serial delta not unique")
	}
	return server.DiskIDs, nil
}
