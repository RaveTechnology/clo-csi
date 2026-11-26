package registry

import (
	"context"
	"testing"
)

func TestRetirementAndExistingBarrierCannotBeReopened(t *testing.T) {
	for _, field := range []string{"retired", "barrier", "proof"} {
		t.Run(field, func(t *testing.T) {
			ctx := context.Background()
			r, _, _ := fresh(t)
			v, e := r.CreateVolume(ctx, volumeSpec("retained-initialization"))
			if e != nil {
				t.Fatal(e)
			}
			v.Status.Retired = true
			v.Status.Barrier = true
			v.Status.RetirementProofSHA256 = "saved-proof-digest"
			v, e = r.SaveVolume(ctx, v, v.Version)
			if e != nil {
				t.Fatal(e)
			}
			wrong := v
			if field == "retired" {
				wrong.Status.Retired = false
			} else if field == "proof" {
				wrong.Status.RetirementProofSHA256 = "replacement-proof"
			} else {
				wrong.Status.Barrier = false
			}
			if _, e := r.SaveVolume(ctx, wrong, wrong.Version); e == nil {
				t.Fatal("permanent retirement reopened", field)
			}
		})
	}
}
