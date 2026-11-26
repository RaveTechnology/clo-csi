package coordinator

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
)

type retirementChannel struct {
	protocol.Channel
	change   func(protocol.Job, *protocol.Receipt)
	quiesces int
}

func (c *retirementChannel) Exchange(ctx context.Context, job protocol.Job) (protocol.Receipt, error) {
	r, e := c.Channel.Exchange(ctx, job)
	if job.Type == "quiesceBlank" {
		c.quiesces++
	}
	if e == nil && c.change != nil {
		c.change(job, &r)
	}
	return r, e
}

func liveRetirement(h *harness, v registry.VolumeRecord, op registry.OperationRecord) protocol.RecoveryRequest {
	q := retirementRequest(h, v, op)
	q.Action = "retire-quiesced-format"
	return q
}

func TestQuiescedFormatRetirementRetainsHistoryWithoutStoppingVM(t *testing.T) {
	ctx := context.Background()
	h := setup(t, true)
	v, op := interruptedFormat(t, h)
	ch := &retirementChannel{Channel: h.channel}
	h.c.nodes = ch
	beforeCalls := []int{h.api.createCalls, h.api.attachCalls, h.api.detachCalls, h.api.deleteCalls, h.api.powerCalls}
	q := liveRetirement(h, v, op)
	result, e := h.c.Recover(ctx, q)
	if e != nil {
		t.Fatal("live unused format retirement failed", e)
	}
	got, _ := h.r.Volume(ctx, v.Name)
	node, _ := h.r.Node(ctx, h.node.BindingID)
	if !got.Status.Retired || !got.Status.Barrier || got.Status.Initialization != "FormatDispatched" || got.Status.FormatOp != op.Name || got.Status.Serial != v.Status.Serial || got.Status.FSUUID != v.Status.FSUUID || got.Status.AttachEpoch != v.Status.AttachEpoch || got.Status.PendingOp != "" || node.Status.PendingEnrollment != "" || node.Status.Fenced {
		t.Fatal("retirement changed history, fenced VM, or left its claims")
	}
	if result.Status.Phase != registry.Completed || !result.Status.Sent || result.Spec.Nonce != op.Spec.Nonce || result.Status.Receipt == nil || !result.Status.Receipt.Quiesced || result.Status.Receipt.Volume.Handle != q.VolumeID || ch.quiesces != 1 {
		t.Fatal("missing durable exact quiescence proof/completion")
	}
	if !reflect.DeepEqual(beforeCalls, []int{h.api.createCalls, h.api.attachCalls, h.api.detachCalls, h.api.deleteCalls, h.api.powerCalls}) {
		t.Fatal("retirement dispatched cloud mutation")
	}
	if _, e = h.c.Recover(ctx, q); e != nil || ch.quiesces != 1 {
		t.Fatal("completed retry replayed quiescence", e)
	}
	if _, e = h.c.Publish(ctx, q.VolumeID, EncodeNodeID(h.node), false); e == nil {
		t.Fatal("retired volume published")
	}
	if e = h.c.Delete(ctx, q.VolumeID); e == nil || h.api.deleteCalls != 0 {
		t.Fatal("retired disk deleted")
	}
	if e = h.c.Unpublish(ctx, q.VolumeID, EncodeNodeID(h.node)); e != nil {
		t.Fatal("normal hot detach of retained disk failed", e)
	}
	if h.api.detachCalls != 1 || h.api.powerCalls != 0 {
		t.Fatal("retained detach used wrong path")
	}
	if _, e = h.c.Recover(ctx, q); e != nil {
		t.Fatal("completed retry after normal detach failed", e)
	}
	if e = h.c.Freeze(ctx); e != nil {
		t.Fatal(e)
	}
	blob, e := h.r.Export(ctx)
	if e != nil {
		t.Fatal(e)
	}
	restored := registry.New(registry.NewMemoryBackend(), h.r.Installation())
	if e = restored.SetFrozen(ctx, true); e != nil {
		t.Fatal(e)
	}
	if e = restored.Restore(ctx, blob); e != nil {
		t.Fatal("retired receipt broke backup graph", e)
	}
}

func TestQuiescedFormatDryRunDoesNotCloseLocalBarrierOrChangeRecords(t *testing.T) {
	h := setup(t, true)
	v, op := interruptedFormat(t, h)
	ch := &retirementChannel{Channel: h.channel}
	h.c.nodes = ch
	ctx := context.Background()
	beforeV, _ := h.r.Volume(ctx, v.Name)
	beforeN, _ := h.r.Node(ctx, h.node.BindingID)
	beforeOp, _ := h.r.Operation(ctx, op.Name)
	q := liveRetirement(h, v, op)
	q.DryRun = true
	if _, e := h.c.Recover(ctx, q); e != nil {
		t.Fatal("read-only eligibility failed", e)
	}
	afterV, _ := h.r.Volume(ctx, v.Name)
	afterN, _ := h.r.Node(ctx, h.node.BindingID)
	afterOp, _ := h.r.Operation(ctx, op.Name)
	if !reflect.DeepEqual(beforeV, afterV) || !reflect.DeepEqual(beforeN, afterN) || !reflect.DeepEqual(beforeOp, afterOp) || ch.quiesces != 0 {
		t.Fatal("dry run mutated records or sent quiesce")
	}
}

func TestQuiescedFormatRefusesUnsafeCurrentFacts(t *testing.T) {
	for _, scenario := range []string{"initialized", "volume-quarantine", "node-quarantine", "fenced", "stopped", "foreign-volume", "wrong-operation", "active-preparer", "unreachable", "mounted", "kernel-busy", "old-boot", "foreign-boot", "foreign-node", "foreign-volume-receipt", "duplicate-device", "root-device", "partitioned", "foreign-signature", "own-filesystem", "filesystem-arrived-during-quiesce", "no-kernel-ack", "newer-claim", "unexpected-fence"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			h := setup(t, true)
			v, op := interruptedFormat(t, h)
			q := liveRetirement(h, v, op)
			ch := &retirementChannel{Channel: h.channel}
			h.c.nodes = ch
			switch scenario {
			case "initialized":
				v.Status.Initialization = "FormatComplete"
				if _, e := h.r.SaveVolume(ctx, v, v.Version); e != nil {
					t.Fatal(e)
				}
			case "volume-quarantine":
				v.Status.Quarantined = true
				if _, e := h.r.SaveVolume(ctx, v, v.Version); e != nil {
					t.Fatal(e)
				}
			case "node-quarantine", "fenced":
				n, _ := h.r.Node(ctx, h.node.BindingID)
				if scenario == "fenced" {
					n.Status.Fenced = true
				} else {
					n.Status.Quarantined = true
				}
				if _, e := h.r.SaveNode(ctx, n, n.Version); e != nil {
					t.Fatal(e)
				}
			case "stopped":
				h.api.server.Status = "STOPPED"
				h.api.server.SwitchStatus = "OFF"
			case "foreign-volume":
				q.VolumeID = h.r.Handle("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee")
			case "wrong-operation":
				q.OperationID = "missing-operation"
			case "active-preparer":
				defer h.c.trackPreparation(op.Name)()
			case "unreachable":
				h.channel.unavailable = true
			case "mounted", "kernel-busy":
				h.channel.mounted = true
			case "newer-claim":
				n, _ := h.r.Node(ctx, h.node.BindingID)
				n.Status.PendingEnrollment = "newer-operation"
				if _, e := h.r.SaveNode(ctx, n, n.Version); e != nil {
					t.Fatal(e)
				}
			case "unexpected-fence":
				q.FenceID = "unexpected"
			default:
				ch.change = func(job protocol.Job, r *protocol.Receipt) {
					if job.Type != "probeBlank" && job.Type != "quiesceBlank" {
						return
					}
					if scenario == "old-boot" && job.Type == "quiesceBlank" {
						r.Snapshot.BootID = "old-boot"
					}
					if scenario == "foreign-boot" {
						r.Snapshot.BootID = "foreign-boot"
					}
					if scenario == "foreign-node" {
						r.Node.UID = "foreign"
					}
					if scenario == "foreign-volume-receipt" {
						r.Volume.AttachEpoch++
					}
					if scenario == "no-kernel-ack" && job.Type == "quiesceBlank" {
						r.Quiesced = false
					}
					for i, d := range r.Snapshot.Disks {
						if d.Serial != v.Status.Serial {
							continue
						}
						switch scenario {
						case "duplicate-device":
							r.Snapshot.Disks = append(r.Snapshot.Disks, d)
						case "root-device":
							r.Snapshot.Disks[i].Root = true
						case "partitioned":
							r.Snapshot.Disks[i].Partitioned = true
						case "foreign-signature":
							r.Snapshot.Disks[i].FSType = "xfs"
							r.Snapshot.Disks[i].FSUUID = "foreign"
						case "own-filesystem":
							r.Snapshot.Disks[i].FSType = "ext4"
							r.Snapshot.Disks[i].FSUUID = v.Status.FSUUID
						case "filesystem-arrived-during-quiesce":
							if job.Type == "quiesceBlank" {
								r.Snapshot.Disks[i].FSType = "ext4"
								r.Snapshot.Disks[i].FSUUID = v.Status.FSUUID
							}
						}
					}
				}
			}
			if _, e := h.c.Recover(ctx, q); e == nil {
				t.Fatal("unsafe retirement admitted", scenario)
			}
			got, _ := h.r.Volume(ctx, v.Name)
			closed, _ := h.r.Operation(ctx, op.Name)
			if got.Status.Retired || closed.Status.Phase == registry.Completed || h.api.powerCalls != 0 || h.api.detachCalls != 0 || h.api.deleteCalls != 0 {
				t.Fatal("rejected retirement changed data/claims")
			}
		})
	}
}

func TestQuiescedRetirementResumesAfterProofTombstoneCompletionAndRelease(t *testing.T) {
	for _, point := range []string{"proof", "tombstone", "completion", "release"} {
		t.Run(point, func(t *testing.T) {
			ctx := context.Background()
			b := &afterCASBackend{Backend: registry.NewMemoryBackend()}
			h := setupBackend(t, true, b)
			v, op := interruptedFormat(t, h)
			q := liveRetirement(h, v, op)
			cancelled, cancel := context.WithCancel(ctx)
			defer cancel()
			interrupted := false
			b.after = func(d registry.Document) {
				switch {
				case point == "proof" && d.Kind == registry.KindOperation:
					var s registry.OperationStatus
					_ = json.Unmarshal(d.Status, &s)
					interrupted = s.Receipt != nil && s.Receipt.Quiesced && s.Phase != registry.Completed
				case point == "tombstone" && d.Kind == registry.KindVolume:
					var s registry.VolumeStatus
					_ = json.Unmarshal(d.Status, &s)
					interrupted = s.Retired
				case point == "completion" && d.Kind == registry.KindOperation:
					var s registry.OperationStatus
					_ = json.Unmarshal(d.Status, &s)
					interrupted = s.Phase == registry.Completed
				case point == "release" && d.Kind == registry.KindVolume:
					var s registry.VolumeStatus
					_ = json.Unmarshal(d.Status, &s)
					interrupted = s.Retired && s.PendingOp == ""
				}
				if interrupted {
					b.after = nil
					cancel()
				}
			}
			if _, e := h.c.Recover(cancelled, q); e == nil || !interrupted {
				t.Fatal("persistent interruption not exercised", point, e)
			}
			if _, e := h.c.Recover(ctx, q); e != nil {
				t.Fatal("matching recovery did not resume", point, e)
			}
			got, _ := h.r.Volume(ctx, v.Name)
			n, _ := h.r.Node(ctx, h.node.BindingID)
			closed, _ := h.r.Operation(ctx, op.Name)
			if !got.Status.Retired || !got.Status.Barrier || got.Status.PendingOp != "" || n.Status.PendingEnrollment != "" || closed.Status.Phase != registry.Completed || !closed.Status.Sent || closed.Status.Receipt == nil || !closed.Status.Receipt.Quiesced || h.api.detachCalls != 0 || h.api.powerCalls != 0 {
				t.Fatal("resumed retirement violated retention/claim contract")
			}
		})
	}
}

func TestQuiescedRetirementRejectsChangedNonceAndSavedProof(t *testing.T) {
	ctx := context.Background()
	b := &afterCASBackend{Backend: registry.NewMemoryBackend()}
	h := setupBackend(t, true, b)
	v, op := interruptedFormat(t, h)
	q := liveRetirement(h, v, op)
	cancelled, cancel := context.WithCancel(ctx)
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
	if _, e := h.c.Recover(cancelled, q); e == nil {
		t.Fatal("tombstone interruption missed")
	}
	got, _ := h.r.Volume(ctx, v.Name)
	if !got.Status.Retired {
		t.Fatal("retained tombstone not reached")
	}
	wrong := q
	wrong.Operator = "different-operator"
	if _, e := h.c.Recover(ctx, wrong); e == nil {
		t.Fatal("changed request reused nonce")
	}
	d, _ := b.Get(ctx, registry.KindOperation, op.Name)
	var status registry.OperationStatus
	_ = json.Unmarshal(d.Status, &status)
	if status.Receipt == nil {
		t.Fatal("matching quiescence proof not persisted")
	}
	status.Receipt.Node.UID = "foreign"
	d.Status, _ = json.Marshal(status)
	if _, e := b.CAS(ctx, d, d.Version); e != nil {
		t.Fatal(e)
	}
	if _, e := h.c.Recover(ctx, q); e == nil {
		t.Fatal("foreign saved quiescence proof resumed tombstone")
	}
	n, _ := h.r.Node(ctx, h.node.BindingID)
	if n.Status.PendingEnrollment != op.Name {
		t.Fatal("mismatched proof released node claim")
	}
}

func TestQuiescedRetirementPreservesNewerUnrelatedNodeClaim(t *testing.T) {
	ctx := context.Background()
	h := setup(t, true)
	v, op := interruptedFormat(t, h)
	q := liveRetirement(h, v, op)
	if _, e := h.c.Recover(ctx, q); e != nil {
		t.Fatal(e)
	}
	other := create(t, h, "other-pvc")
	n, _ := h.r.Node(ctx, h.node.BindingID)
	later, done, e := h.c.newOperation(ctx, "extend", other, &n, h.channel.snapshot(), nil, 20)
	defer done()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = h.c.Recover(ctx, q); e != nil {
		t.Fatal("completed recovery could not retry", e)
	}
	n, _ = h.r.Node(ctx, h.node.BindingID)
	other, _ = h.r.Volume(ctx, other.Name)
	if n.Status.PendingEnrollment != later.Name || other.Status.PendingOp != later.Name {
		t.Fatal("old recovery erased unrelated new claims")
	}
}

func TestQuiescedRetirementRejectsUnavailableRegistryAuthority(t *testing.T) {
	for _, scenario := range []string{"frozen", "restore-needs-fence", "unknown-issuer", "unsent"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			b := registry.NewMemoryBackend()
			h := setupBackend(t, true, b)
			var v registry.VolumeRecord
			var op registry.OperationRecord
			if scenario == "unsent" {
				v, op = preparedFormat(t, h)
			} else {
				v, op = interruptedFormat(t, h)
			}
			q := liveRetirement(h, v, op)
			switch scenario {
			case "frozen":
				if e := h.r.SetFrozen(ctx, true); e != nil {
					t.Fatal(e)
				}
			case "restore-needs-fence":
				d, _ := b.Get(ctx, registry.KindIssuer, "installation")
				var s registry.IssuerStatus
				_ = json.Unmarshal(d.Status, &s)
				s.RestoreNeedsFence = true
				d.Status, _ = json.Marshal(s)
				if _, e := b.CAS(ctx, d, d.Version); e != nil {
					t.Fatal(e)
				}
			case "unknown-issuer":
				i, _ := h.r.Issuer(ctx)
				i.Status.Current.ProcessNonce = "unknown-process"
				h.c.mu.Lock()
				h.c.issuer = i.Status.Current
				h.c.mu.Unlock()
				d, _ := b.Get(ctx, registry.KindIssuer, "installation")
				d.Status, _ = json.Marshal(i.Status)
				if _, e := b.CAS(ctx, d, d.Version); e != nil {
					t.Fatal(e)
				}
			}
			if _, e := h.c.Recover(ctx, q); e == nil {
				t.Fatal("invalid authority accepted", scenario)
			}
			got, _ := h.r.Volume(ctx, v.Name)
			if got.Status.Retired || got.Status.PendingOp != op.Name || h.api.powerCalls != 0 || h.api.deleteCalls != 0 {
				t.Fatal("rejected authority changed original state")
			}
		})
	}
}
