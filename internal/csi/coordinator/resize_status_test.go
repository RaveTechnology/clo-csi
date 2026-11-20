package coordinator

import (
	"context"
	"testing"

	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
)

func TestExpandReportedTargetSizeRequiresReadyProviderState(t *testing.T) {
	for _, status := range []string{"ERROR", "PROCESSING"} {
		t.Run(status, func(t *testing.T) {
			h := setup(t, true)
			v := create(t, h, "unready-reported-size")
			h.api.mu.Lock()
			a := h.api.volumes[v.Status.ProviderID]
			a.SizeGB, a.Status = 12, status
			h.api.volumes[a.ID] = a
			h.api.mu.Unlock()
			size, err := h.c.Expand(context.Background(), h.r.Handle(v.Status.ProviderID), 12<<30, 0)
			if err == nil || size != 0 || h.api.extendCalls != 0 {
				t.Fatalf("unready reported capacity accepted or expansion resent: size=%d err=%v calls=%d", size, err, h.api.extendCalls)
			}
		})
	}
}

func TestExpandConflictCannotAcceptUnreadyReportedCapacity(t *testing.T) {
	for _, status := range []string{"ERROR", "PROCESSING"} {
		t.Run(status, func(t *testing.T) {
			b := claimBackend(func(s registry.OperationSpec) bool { return s.Type == "extend" && s.TargetSizeGB == 11 })
			h := setupBackend(t, true, b)
			v := published(t, h)
			handle := h.r.Handle(v.Status.ProviderID)
			b.armed.Store(true)
			type result struct {
				size int64
				err  error
			}
			done := make(chan result, 1)
			go func() { size, err := h.c.Expand(context.Background(), handle, 11<<30, 0); done <- result{size, err} }()
			waitClaim(t, b)
			if size, err := h.c.Expand(context.Background(), handle, 12<<30, 0); err != nil || size != 12<<30 {
				close(b.resume)
				t.Fatalf("competing ready expansion failed: %d %v", size, err)
			}
			h.api.mu.Lock()
			a := h.api.volumes[v.Status.ProviderID]
			a.Status = status
			h.api.volumes[a.ID] = a
			h.api.mu.Unlock()
			close(b.resume)
			got := <-done
			if got.err == nil || got.size != 0 || h.api.extendCalls != 1 {
				t.Fatalf("conflict accepted unready capacity or replayed native extend: %+v calls=%d", got, h.api.extendCalls)
			}
		})
	}
}
