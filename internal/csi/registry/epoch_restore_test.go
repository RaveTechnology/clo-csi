package registry

import (
	"context"
	"encoding/json"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"testing"
)

func TestRestoreNeverRewindsExistingLiveIssuer(t *testing.T) {
	ctx := context.Background()
	r, _, seed := fresh(t)
	_, _ = r.CreateVolume(ctx, volumeSpec("pvc-a"))
	_ = r.SetFrozen(ctx, true)
	blob, e := r.Export(ctx)
	if e != nil {
		t.Fatal(e)
	}
	next := seed.Issuer
	next.ProcessNonce = "new-process"
	next.HostPID = 71
	next.StartTicks = 2000
	proof := &protocol.IssuerProof{Descriptor: seed.Issuer, Verdict: protocol.Gone}
	if e = r.ActivateIssuer(ctx, next, proof); e != nil {
		t.Fatal(e)
	}
	if e = r.Restore(ctx, blob); e != nil {
		t.Fatal(e)
	}
	current, e := r.Issuer(ctx)
	if e != nil || current.Status.Current != next {
		t.Fatal("restore rewound latest known issuer", e)
	}
}
func TestMissingEpochCannotInitializeOverOwnedRecords(t *testing.T) {
	ctx := context.Background()
	r, b, seed := fresh(t)
	_, _ = r.CreateVolume(ctx, volumeSpec("pvc-a"))
	b.mu.Lock()
	delete(b.docs, KindIssuer+"/installation")
	b.mu.Unlock()
	next := seed.Issuer
	next.ProcessNonce = "new-process"
	if e := r.ActivateIssuer(ctx, next, nil); e == nil {
		t.Fatal("missing epoch reset authority while ownership records remain")
	}
}
func TestRestoreWithoutLiveEpochRequiresRecoveryFence(t *testing.T) {
	ctx := context.Background()
	r, _, seed := fresh(t)
	_, _ = r.CreateVolume(ctx, volumeSpec("pvc-a"))
	_ = r.SetFrozen(ctx, true)
	blob, e := r.Export(ctx)
	if e != nil {
		t.Fatal(e)
	}
	r2 := New(NewMemoryBackend(), r.installation)
	_ = r2.SetFrozen(ctx, true)
	if e = r2.Restore(ctx, blob); e != nil {
		t.Fatal(e)
	}
	current, e := r2.Issuer(ctx)
	if e != nil || !current.Status.RestoreNeedsFence {
		t.Fatal("lost epoch restored without unknown-writer guard", e)
	}
	next := seed.Issuer
	next.ProcessNonce = "new-process"
	if e = r2.ActivateIssuer(ctx, next, &protocol.IssuerProof{Descriptor: seed.Issuer, Verdict: protocol.Gone}); e == nil {
		t.Fatal("historical backup issuer proof bypassed lost live epoch")
	}
}

func TestStaleOperationUIDCannotReleaseRestoredLocks(t *testing.T) {
	ctx := context.Background()
	r, b, old := prepared(t)
	d, e := b.Get(ctx, KindOperation, old.Name)
	if e != nil {
		t.Fatal(e)
	}
	var status OperationStatus
	_ = json.Unmarshal(d.Status, &status)
	status.Phase = Completed
	d.Status, _ = json.Marshal(status)
	b.mu.Lock()
	delete(b.docs, KindOperation+"/"+old.Name)
	b.mu.Unlock()
	if _, e = b.Create(ctx, d); e != nil {
		t.Fatal(e)
	}
	if e = r.Release(ctx, old); e == nil {
		t.Fatal("stale operation UID released restored locks")
	}
	v, _ := r.Volume(ctx, old.Spec.VolumeRecordID)
	if v.Status.PendingOp != old.Name {
		t.Fatal("restored lock cleared")
	}
}
