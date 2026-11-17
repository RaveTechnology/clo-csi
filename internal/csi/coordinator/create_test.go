package coordinator

import (
	"context"
	"errors"
	"github.com/RaveTechnology/clo-cloud-go/cloud"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"math"
	"testing"
)

func TestCreateIdempotencyAndCapacityRange(t *testing.T) {
	ctx := context.Background()
	h := setup(t, true)
	a := create(t, h, "pvc-a")
	q := request("pvc-a")
	q.RequiredBytes = 9 << 30
	q.LimitBytes = 12 << 30
	b, e := h.c.EnsureVolume(ctx, q)
	if e != nil || a.Status.ProviderID != b.Status.ProviderID || h.api.createCalls != 1 {
		t.Fatalf("not idempotent %+v %v", b, e)
	}
	q.RequiredBytes = 11 << 30
	_, e = h.c.EnsureVolume(ctx, q)
	mustBlocked(t, e)
	for _, bounds := range [][2]int64{{-1, 0}, {1, 1}, {12 << 30, 10 << 30}, {math.MaxInt64, 0}, {0, -1}} {
		q = request("bad")
		q.RequiredBytes = bounds[0]
		q.LimitBytes = bounds[1]
		_, e = h.c.EnsureVolume(ctx, q)
		mustBlocked(t, e)
	}
	if h.api.createCalls != 1 {
		t.Fatal("invalid range reached cloud")
	}
}
func TestCreateUncertainZeroOneMultipleMatches(t *testing.T) {
	for _, matches := range []int{0, 1, 2} {
		t.Run(string(rune('0'+matches)), func(t *testing.T) {
			h := setup(t, true)
			h.api.createErr = errors.New("lost response")
			h.api.createBeforeErr = matches > 0
			_, e := h.c.EnsureVolume(context.Background(), request("pvc-a"))
			mustBlocked(t, e)
			if matches == 2 {
				h.api.extra = []cloud.Volume{{ID: "foreign", Name: h.r.ProviderName("pvc-a"), Status: "AVAILABLE", SizeGB: 10}}
			}
			v, e := h.c.EnsureVolume(context.Background(), request("pvc-a"))
			if matches == 1 && e != nil {
				t.Fatal(e)
			}
			if matches != 1 && e == nil {
				t.Fatal("ambiguous create recovered")
			}
			if h.api.createCalls != 1 {
				t.Fatal("create POST repeated")
			}
			if matches != 1 {
				v, e = h.r.Volume(context.Background(), registry.Name("v", "pvc-a"))
				if e != nil || v.Status.PendingOp == "" {
					t.Fatal("uncertain lock lost")
				}
			}
		})
	}
}
func TestNameConflictRequiresSameOwner(t *testing.T) {
	h := setup(t, true)
	_ = create(t, h, "pvc-a")
	q := request("pvc-a")
	q.FSType = "xfs"
	_, e := h.c.EnsureVolume(context.Background(), q)
	mustBlocked(t, e)
	h.api.volumes["foreign"] = cloud.Volume{ID: "foreign", Name: h.r.ProviderName("pvc-b"), Status: "AVAILABLE", SizeGB: 10}
	_, e = h.c.EnsureVolume(context.Background(), request("pvc-b"))
	mustBlocked(t, e)
	if h.api.createCalls != 1 {
		t.Fatal("name conflict reached POST")
	}
}
func TestWrongProject404NeverSucceeds(t *testing.T) {
	h := setup(t, true)
	v := create(t, h, "pvc-a")
	h.api.projectErr = &cloud.APIError{Status: 404}
	e := h.c.Delete(context.Background(), h.r.Handle(v.Status.ProviderID))
	mustBlocked(t, e)
	if h.api.deleteCalls != 0 {
		t.Fatal("wrong project deleted volume")
	}
}
