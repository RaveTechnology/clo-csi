package node

import (
	"context"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"reflect"
	"testing"
)

type kernelBusyFilesystem struct{ Filesystem }

func (kernelBusyFilesystem) VerifyUnmounted(context.Context, Device) error {
	return protocol.Fail(protocol.Blocked, "kernel reports device busy outside host mount inventory")
}

func TestQuiesceRejectsKernelBusyWithEmptyHostMountInventory(t *testing.T) {
	h := newFS(t, true)
	r := &Runtime{Inspector: h.ins, Journal: h.j, Node: h.r.Node, Filesystem: kernelBusyFilesystem{h.fs}}
	job := protocol.Job{ID: "hidden-namespace", Nonce: "nonce", Type: "quiesce", Node: h.p.Node, Volume: h.p.Volume}
	receipt, e := r.Execute(context.Background(), job)
	if e == nil || receipt.Quiesced {
		t.Fatal("detach receipt trusted host-only mount inventory despite busy kernel device")
	}
}
func TestQuiesceRequiresKernelDeviceVerifier(t *testing.T) {
	h := newFS(t, true)
	r := &Runtime{Inspector: h.ins, Journal: h.j, Node: h.r.Node}
	receipt, e := r.Execute(context.Background(), protocol.Job{ID: "no-kernel-probe", Nonce: "nonce", Type: "quiesce", Node: h.p.Node, Volume: h.p.Volume})
	if e == nil || receipt.Quiesced {
		t.Fatal("detach receipt issued without kernel device probe")
	}
}

func TestRepeatedJobReturnsSameReceipt(t *testing.T) {
	ctx := context.Background()
	ls, fm, boot := captured(t, "order-0-hot")
	s, e := ParseSnapshot(ls, fm, boot)
	if e != nil {
		t.Fatal(e)
	}
	ins := &fixedInspector{snapshot: s}
	j, e := NewJournal(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	n := protocol.NodeRef{UID: "node-a", BindingID: "binding-a", ServerID: "server-a", Epoch: 1}
	r := &Runtime{Inspector: ins, Journal: j, Node: func() protocol.NodeRef { return n }}
	job := protocol.Job{ID: "job-a", Nonce: "nonce-a", Type: "baseline", Node: n}
	a, e := r.Execute(ctx, job)
	if e != nil {
		t.Fatal(e)
	}
	restarted := &Runtime{Inspector: ins, Journal: j, Node: r.Node}
	b, e := restarted.Execute(ctx, job)
	if e != nil || !reflect.DeepEqual(a, b) || ins.calls != 1 {
		t.Fatalf("job repeated: %+v %+v %v calls%d", a, b, e, ins.calls)
	}
	job.Nonce = "reused-id"
	if _, e = r.Execute(ctx, job); e == nil {
		t.Fatal("changed job with same ID accepted")
	}
	job.ID = "foreign"
	job.Node.UID = "another-node"
	if _, e = r.Execute(ctx, job); e == nil {
		t.Fatal("foreign job executed")
	}
}
