package channel

import (
	"context"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"testing"
	"time"
)

func TestNodeCannotChangeJobEpochOrReceiptOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	q := NewQueue()
	n := protocol.NodeRef{UID: nodeUID, BindingID: "binding", ServerID: "server", Epoch: 1}
	job := protocol.Job{ID: "job-a", Nonce: "nonce-a", Node: n, Type: "observe"}
	done := make(chan error, 1)
	go func() { _, e := q.Exchange(ctx, job); done <- e }()
	got, e := q.Poll(ctx, n)
	if e != nil || got.Nonce != job.Nonce {
		t.Fatalf("poll %+v %v", got, e)
	}
	r := protocol.Receipt{JobID: job.ID, Nonce: job.Nonce, Type: job.Type, Node: n}
	wrong := n
	wrong.Epoch++
	if e = q.Complete(ctx, wrong, r); e == nil {
		t.Fatal("foreign caller completed job")
	}
	r.Node = wrong
	if e = q.Complete(ctx, n, r); e == nil {
		t.Fatal("modified job epoch accepted")
	}
	r.Node = n
	r.Nonce = "another"
	if e = q.Complete(ctx, n, r); e == nil {
		t.Fatal("modified nonce accepted")
	}
	r.Nonce = job.Nonce
	if e = q.Complete(ctx, n, r); e != nil {
		t.Fatal(e)
	}
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	if e = q.Complete(ctx, n, r); e != nil {
		t.Fatal("same receipt is not idempotent", e)
	}
}
