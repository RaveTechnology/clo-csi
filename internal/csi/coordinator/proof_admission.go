package coordinator

import (
	"context"

	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
)

// Fail fast rather than enqueue repeated agent inventories behind the shared
// cloud pacer. Recovery binding, polling and job receipts do not take this gate.
func (c *Coordinator) admitProof(ctx context.Context) (func(), error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	if !c.proofMu.TryLock() {
		return nil, protocol.Fail(protocol.Conflict, "fresh node proof busy; retry")
	}
	return c.proofMu.Unlock, nil
}
