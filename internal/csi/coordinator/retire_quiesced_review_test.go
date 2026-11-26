package coordinator

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
)

func TestLiveRetirementRejectsReenrolledBootAndMissingBlankProof(t *testing.T) {
	for _, scenario := range []string{"reenrolled-boot", "no-blank-proof", "old-agent"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			h := setup(t, true)
			v, op := interruptedFormat(t, h)
			ch := &retirementChannel{Channel: h.channel}
			h.c.nodes = ch
			if scenario == "reenrolled-boot" {
				h.channel.bootID = "rebooted-host"
				n, _ := h.r.Node(ctx, h.node.BindingID)
				n.Status.BootID = "rebooted-host"
				if _, e := h.r.SaveNode(ctx, n, n.Version); e != nil {
					t.Fatal(e)
				}
			}
			if scenario == "no-blank-proof" {
				ch.change = func(_ protocol.Job, r *protocol.Receipt) {
					raw, _ := json.Marshal(r)
					var wire map[string]any
					_ = json.Unmarshal(raw, &wire)
					delete(wire, "blankChecked")
					raw, _ = json.Marshal(wire)
					*r = protocol.Receipt{}
					_ = json.Unmarshal(raw, r)
				}
			}
			if scenario == "old-agent" {
				ch.change = func(j protocol.Job, r *protocol.Receipt) {
					if j.Type == "probeBlank" || j.Type == "quiesceBlank" {
						r.Error = "unsupported node job"
					}
				}
			}
			if _, e := h.c.Recover(ctx, liveRetirement(h, v, op)); e == nil {
				t.Fatal("unproven boot/blank authority retired disk", scenario)
			}
			got, _ := h.r.Volume(ctx, v.Name)
			n, _ := h.r.Node(ctx, h.node.BindingID)
			if got.Status.Retired || got.Status.PendingOp != op.Name || n.Status.PendingEnrollment != op.Name {
				t.Fatal("failed proof released claims")
			}
			if scenario == "reenrolled-boot" && ch.quiesces != 0 {
				t.Fatal("rebooted node quiesced")
			}
		})
	}
}

func TestLiveRetirementRejectsTamperedDurableProof(t *testing.T) {
	for _, scenario := range []string{"boot", "other-job", "receipt-time", "missing-request"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			b := &afterCASBackend{Backend: registry.NewMemoryBackend()}
			h := setupBackend(t, true, b)
			v, op := interruptedFormat(t, h)
			q := liveRetirement(h, v, op)
			interrupted, cancel := context.WithCancel(ctx)
			defer cancel()
			b.after = func(d registry.Document) {
				if d.Kind == registry.KindVolume {
					var s registry.VolumeStatus
					_ = json.Unmarshal(d.Status, &s)
					if s.Retired {
						b.after = nil
						cancel()
					}
				}
			}
			if _, e := h.c.Recover(interrupted, q); e == nil {
				t.Fatal("tombstone interruption missed")
			}
			d, _ := b.Get(ctx, registry.KindOperation, op.Name)
			var s registry.OperationStatus
			_ = json.Unmarshal(d.Status, &s)
			if s.Receipt == nil {
				t.Fatal("proof absent")
			}
			switch scenario {
			case "boot":
				s.Receipt.Snapshot.BootID = "another-boot"
			case "other-job":
				s.Receipt.JobID = "another-job"
				s.Receipt.Nonce = "another-nonce"
			case "receipt-time":
				s.Receipt.ObservedAt = s.Receipt.ObservedAt.Add(time.Second)
			case "missing-request":
				s.RecoveryRequests = nil
			}
			d.Status, _ = json.Marshal(s)
			if _, e := b.CAS(ctx, d, d.Version); e != nil {
				t.Fatal(e)
			}
			if _, e := h.c.Recover(ctx, q); e == nil {
				t.Fatal("tampered proof resumed retirement", scenario)
			}
			got, _ := h.r.Volume(ctx, v.Name)
			n, _ := h.r.Node(ctx, h.node.BindingID)
			closed, _ := h.r.Operation(ctx, op.Name)
			if got.Status.PendingOp != op.Name || n.Status.PendingEnrollment != op.Name || closed.Status.Phase == registry.Completed {
				t.Fatal("tampered receipt released claims")
			}
		})
	}
}
