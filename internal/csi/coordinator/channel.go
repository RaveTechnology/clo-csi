package coordinator

import (
	"context"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
)

func (c *Coordinator) PollNodeJob(ctx context.Context, node protocol.NodeRef) (protocol.Job, error) {
	queue, ok := c.nodes.(protocol.PollingChannel)
	if !ok {
		return protocol.Job{}, protocol.Fail(protocol.Unavailable, "node polling channel unavailable")
	}
	return queue.Poll(ctx, node)
}
func (c *Coordinator) ReceiveJobReceipt(ctx context.Context, node protocol.NodeRef, r protocol.Receipt) error {
	queue, ok := c.nodes.(protocol.PollingChannel)
	if !ok {
		return protocol.Fail(protocol.Unavailable, "node polling channel unavailable")
	}
	return queue.Complete(ctx, node, r)
}
func (c *Coordinator) LookupVolume(ctx context.Context, handle string) (registry.VolumeRecord, error) {
	v, e := c.repo.FindVolume(ctx, handle)
	if e != nil {
		return v, e
	}
	if v.Status.Deleted || v.Status.Retired {
		return v, protocol.Fail(protocol.NotFound, "volume no longer usable")
	}
	_, e = c.ownedVolume(ctx, v)
	return v, e
}
