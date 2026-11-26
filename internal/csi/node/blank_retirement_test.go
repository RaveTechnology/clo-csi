package node

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"sync/atomic"
	"testing"
	"time"
)

type rawSignatureFilesystem struct {
	Filesystem
	signatures []Signature
	failure    error
	probes     int
}

func (f *rawSignatureFilesystem) Probe(context.Context, Device) ([]Signature, error) {
	f.probes++
	return f.signatures, f.failure
}

func TestBlankProofRequiresRawProbeAndNoLocalComplete(t *testing.T) {
	for _, kind := range []string{"probeBlank", "quiesceBlank"} {
		for _, scenario := range []string{"blank", "own-ext4-stale-lsblk", "foreign-gpt-stale-lsblk", "probe-error", "local-complete", "kernel-busy", "local-stage", "pending-target"} {
			t.Run(kind+"/"+scenario, func(t *testing.T) {
				h := newFS(t, false)
				ctx := context.Background()
				fs := &rawSignatureFilesystem{Filesystem: h.fs}
				switch scenario {
				case "own-ext4-stale-lsblk":
					fs.signatures = []Signature{{Type: "ext4", UUID: h.p.Volume.FSUUID}}
				case "foreign-gpt-stale-lsblk":
					fs.signatures = []Signature{{Type: "gpt"}}
				case "probe-error":
					fs.failure = errors.New("raw device unavailable")
				case "local-complete", "local-stage", "pending-target":
					s := LocalState{Key: h.p.Volume.Handle, Volume: h.p.Volume, Node: h.p.Node}
					switch scenario {
					case "local-complete":
						s.Initialization = "FormatComplete"
					case "local-stage":
						s.StagePath = h.stage
					case "pending-target":
						s.PendingTargets = map[string]bool{"pod-path": true}
					}
					if e := h.j.Save(s); e != nil {
						t.Fatal(e)
					}
				}
				var verifier Filesystem = fs
				if scenario == "kernel-busy" {
					verifier = kernelBusyFilesystem{fs}
				}
				r := &Runtime{Inspector: h.ins, Journal: h.j, Node: h.r.Node, Filesystem: verifier}
				receipt, e := r.Execute(ctx, protocol.Job{ID: "blank-proof", Nonce: "blank-nonce", Type: kind, Node: h.p.Node, Volume: h.p.Volume})
				if scenario != "blank" {
					if e == nil {
						t.Fatal("unsafe blank proof accepted", scenario)
					}
					return
				}
				if e != nil {
					t.Fatal("blank proof failed", e)
				}
				raw, _ := json.Marshal(receipt)
				var wire map[string]any
				_ = json.Unmarshal(raw, &wire)
				if wire["blankChecked"] != true || fs.probes != 1 || receipt.Quiesced != (kind == "quiesceBlank") {
					t.Fatal("raw affirmative proof missing")
				}
				s, e := h.j.Load(h.p.Volume.Handle)
				if kind == "probeBlank" {
					if e == nil && (s.Barrier || s.DetachCommitted) {
						t.Fatal("read-only proof closed barrier")
					}
				} else {
					if e != nil || !s.Barrier || !s.DetachCommitted {
						t.Fatal("blank quiescence not durable")
					}
					if _, e = h.r.Stage(ctx, h.p, h.stage); e == nil || h.fs.formats != 0 {
						t.Fatal("old permit survived blank barrier")
					}
				}
			})
		}
	}
}

func TestBlankQuiescencePreservesOrdinaryFilesystemDetach(t *testing.T) {
	h := newFS(t, true)
	fs := &rawSignatureFilesystem{Filesystem: h.fs, signatures: []Signature{{Type: "ext4", UUID: h.p.Volume.FSUUID}}}
	r := &Runtime{Inspector: h.ins, Journal: h.j, Node: h.r.Node, Filesystem: fs}
	receipt, e := r.Execute(context.Background(), protocol.Job{ID: "normal-hot-detach", Nonce: "detach-nonce", Type: "quiesce", Node: h.p.Node, Volume: h.p.Volume})
	if e != nil || !receipt.Quiesced || receipt.BlankChecked || fs.probes != 0 {
		t.Fatal("blank proof changed ordinary ext4 detach", e)
	}
}

type staleFormattingFilesystem struct {
	*delayedFormatFilesystem
	inspector  *fsInspector
	rawCreated atomic.Bool
	uuid       string
}

func (f *staleFormattingFilesystem) Format(ctx context.Context, d Device, id string) error {
	if e := f.delayedFormatFilesystem.Format(ctx, d, id); e != nil {
		return e
	}
	f.inspector.mu.Lock()
	for i := range f.inspector.s.Disks {
		if f.inspector.s.Disks[i].Serial == d.Serial {
			f.inspector.s.Disks[i].FSType = ""
			f.inspector.s.Disks[i].FSUUID = ""
		}
	}
	f.inspector.mu.Unlock()
	f.rawCreated.Store(true)
	return nil
}
func (f *staleFormattingFilesystem) Probe(context.Context, Device) ([]Signature, error) {
	if f.rawCreated.Load() {
		return []Signature{{Type: "ext4", UUID: f.uuid}}, nil
	}
	return nil, nil
}

func TestBlankQuiescenceWaitsForFormatterAndRejectsStaleInventory(t *testing.T) {
	h := newFS(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fs := &staleFormattingFilesystem{delayedFormatFilesystem: &delayedFormatFilesystem{Filesystem: h.fs, started: make(chan struct{}), finish: make(chan struct{})}, inspector: h.ins, uuid: h.p.Volume.FSUUID}
	h.r.Filesystem = fs
	staged := make(chan error, 1)
	go func() { _, e := h.r.Stage(ctx, h.p, h.stage); staged <- e }()
	select {
	case <-fs.started:
	case <-ctx.Done():
		t.Fatal("formatter did not begin")
	}
	r := &Runtime{Inspector: h.ins, Journal: h.j, Node: h.r.Node, Filesystem: fs}
	done := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		receipt, e := r.Execute(ctx, protocol.Job{ID: "blank-after-format", Nonce: "blank-nonce", Type: "quiesceBlank", Node: h.p.Node, Volume: h.p.Volume})
		if e == nil && receipt.BlankChecked {
			t.Error("initialized filesystem retired as blank")
		}
		done <- e
	}()
	<-started
	select {
	case e := <-done:
		t.Fatal("blank quiescence did not serialize with active formatter", e)
	case <-time.After(25 * time.Millisecond):
	}
	close(fs.finish)
	if e := <-staged; e != nil {
		t.Fatal("first format failed", e)
	}
	if e := <-done; e == nil {
		t.Fatal("formatted disk passed blank proof with stale lsblk")
	}
	snapshot, _ := h.ins.Snapshot(ctx)
	for _, d := range snapshot.Disks {
		if d.Serial == h.p.Volume.Serial && (d.FSType != "" || d.FSUUID != "") {
			t.Fatal("test did not retain stale inventory")
		}
	}
	if !fs.rawCreated.Load() || h.fs.formats != 1 || h.fs.mounts != 0 {
		t.Fatal("first-format race not exercised")
	}
	if _, e := h.r.Stage(ctx, h.p, h.stage); e == nil || h.fs.formats != 1 {
		t.Fatal("second format dispatched after closed barrier")
	}
}
