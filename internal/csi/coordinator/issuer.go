package coordinator

import (
	"context"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
)

func (c *Coordinator) StartIssuer(ctx context.Context, podUID, nonce string) error {
	c.mu.Lock()
	c.launchPodUID = podUID
	c.launchNonce = nonce
	c.mu.Unlock()
	done, e := c.begin(ctx)
	if e != nil {
		return e
	}
	defer done()
	return c.establishIssuer(ctx, podUID, nonce)
}
func (c *Coordinator) self(ctx context.Context) (protocol.IssuerDescriptor, error) {
	c.mu.RLock()
	pod, nonce := c.launchPodUID, c.launchNonce
	c.mu.RUnlock()
	if pod == "" || nonce == "" {
		return protocol.IssuerDescriptor{}, protocol.Fail(protocol.Blocked, "controller launch identity not registered")
	}
	return c.locateSelf(ctx, pod, nonce)
}
func (c *Coordinator) locateSelf(ctx context.Context, podUID, nonce string) (protocol.IssuerDescriptor, error) {
	// Bootstrap/register/poll remain usable while this method cannot make ready.
	nodes, e := c.repo.Nodes(ctx)
	if e != nil {
		return protocol.IssuerDescriptor{}, e
	}
	var host protocol.NodeRef
	for _, n := range nodes {
		if n.Spec.Ref.UID == c.cfg.ControllerNodeUID && !n.Status.Fenced && !n.Status.Quarantined {
			if host.BindingID != "" {
				return protocol.IssuerDescriptor{}, protocol.Fail(protocol.Blocked, "controller host binding ambiguous")
			}
			host = n.Spec.Ref
		}
	}
	if host.BindingID == "" {
		return protocol.IssuerDescriptor{}, protocol.Fail(protocol.Blocked, "controller requires a verified CLO host agent")
	}
	located, e := c.exchange(ctx, "locateIssuer", host, protocol.VolumeRef{}, protocol.IssuerDescriptor{}, podUID, nonce)
	if e != nil {
		return protocol.IssuerDescriptor{}, e
	}
	next := located.Issuer
	if !next.Valid() || next.PodUID != podUID || next.ProcessNonce != nonce || next.NodeUID != host.UID || next.BindingID != host.BindingID || next.ServerID != host.ServerID {
		return protocol.IssuerDescriptor{}, protocol.Fail(protocol.Blocked, "issuer locator returned different host/process")
	}
	return next, nil
}
func (c *Coordinator) establishIssuer(ctx context.Context, podUID, nonce string) error {
	next, e := c.locateSelf(ctx, podUID, nonce)
	if e != nil {
		return e
	}
	var proof *protocol.IssuerProof
	old, e := c.repo.Issuer(ctx)
	if e != nil && !code(e, protocol.NotFound) {
		return e
	}
	if e == nil && old.Status.Current.Valid() && old.Status.Current != next {
		previous := old.Status.Current
		n, e := c.repo.Node(ctx, previous.BindingID)
		if e != nil {
			return e
		}
		if n.Status.Fenced {
			if e = c.checkNodeFence(ctx, n); e != nil {
				return e
			}
			p := protocol.IssuerProof{Descriptor: previous, Verdict: protocol.Gone, Reason: "explicit VM fence verified"}
			proof = &p
		} else {
			r, e := c.exchange(ctx, "verifyIssuer", n.Spec.Ref, protocol.VolumeRef{}, previous, "", "")
			if e != nil {
				return e
			}
			if r.Proof.Descriptor != previous || r.Proof.Verdict != protocol.Gone {
				return protocol.Fail(protocol.Blocked, "previous controller issuer still Alive/Unknown")
			}
			proof = &r.Proof
		}
	}
	if e = c.audit(ctx); e != nil {
		return e
	}
	if e = c.repo.ActivateIssuer(ctx, next, proof); e != nil {
		return e
	}
	c.mu.Lock()
	c.issuer = next
	c.ready = true
	c.mu.Unlock()
	return nil
}
