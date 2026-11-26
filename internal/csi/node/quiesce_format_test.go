package node

import (
	"context"
	"testing"
	"time"

	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
)

func TestQuiesceBlocksDelayedCapturedFirstFormatPermit(t *testing.T) {
	h := newFS(t, false)
	ctx := context.Background()
	r := &Runtime{Inspector: h.ins, Journal: h.j, Node: h.r.Node, Filesystem: h.fs}
	q := protocol.Job{ID: "revoke-unused-format", Nonce: "quiesce-nonce", Type: "quiesce", Node: h.p.Node, Volume: h.p.Volume}
	receipt, e := r.Execute(ctx, q)
	if e != nil || !receipt.Quiesced {
		t.Fatal("unused device did not quiesce", e)
	}
	if _, e = h.r.Stage(ctx, h.p, h.stage); e == nil || h.fs.formats != 0 || h.fs.mounts != 0 {
		t.Fatal("captured format permit survived committed barrier", e)
	}
	s, e := h.j.Load(h.p.Volume.Handle)
	if e != nil || !s.Barrier || !s.DetachCommitted {
		t.Fatal("quiescence was not durable", e)
	}
}

type delayedFormatFilesystem struct {
	Filesystem
	started, finish chan struct{}
}

func (f *delayedFormatFilesystem) Format(ctx context.Context, d Device, id string) error {
	close(f.started)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.finish:
	}
	return f.Filesystem.Format(ctx, d, id)
}

func TestQuiesceSerializesWithAlreadyRunningFirstFormat(t *testing.T) {
	h := newFS(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fs := &delayedFormatFilesystem{Filesystem: h.fs, started: make(chan struct{}), finish: make(chan struct{})}
	h.r.Filesystem = fs
	staged := make(chan error, 1)
	go func() { _, e := h.r.Stage(ctx, h.p, h.stage); staged <- e }()
	select {
	case <-fs.started:
	case <-ctx.Done():
		t.Fatal("formatter did not start")
	}
	r := &Runtime{Inspector: h.ins, Journal: h.j, Node: h.r.Node, Filesystem: h.fs}
	done := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		receipt, e := r.Execute(ctx, protocol.Job{ID: "quiesce-running-format", Nonce: "quiesce-nonce", Type: "quiesce", Node: h.p.Node, Volume: h.p.Volume})
		if e == nil && !receipt.Quiesced {
			t.Error("missing kernel quiescence ack")
		}
		done <- e
	}()
	<-started
	select {
	case e := <-done:
		t.Fatal("quiesce returned while formatter held volume journal lock", e)
	case <-time.After(25 * time.Millisecond):
	}
	close(fs.finish)
	if e := <-staged; e != nil {
		t.Fatal("original formatter failed", e)
	}
	if e := <-done; e != nil {
		t.Fatal("post-format quiescence failed", e)
	}
	if _, e := h.r.Stage(ctx, h.p, h.stage); e == nil || h.fs.formats != 1 || h.fs.mounts != 0 {
		t.Fatal("captured permit reopened after quiescence", e)
	}
}
