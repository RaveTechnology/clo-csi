package registry

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const installID = "11111111-1111-4111-8111-111111111111"
const projectID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

func fresh(t *testing.T) (*Repository, *MemoryBackend, OperationSpec) {
	t.Helper()
	b := NewMemoryBackend()
	r := New(b, Installation{ID: installID, ProjectID: projectID})
	var seed OperationSpec
	if err := json.Unmarshal([]byte(`{"type":"attach","nonce":"operation-a","issuer":{"podUID":"99999999-9999-4999-8999-999999999999","processNonce":"first-process","nodeUID":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","bindingID":"node-record","serverID":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","hostBootID":"ffffffff-ffff-4fff-8fff-ffffffffffff","hostPID":70,"startTicks":1000},"before":{"bootID":"ffffffff-ffff-4fff-8fff-ffffffffffff","disks":[{"path":"/dev/sda","serial":"root-serial","root":true}]}}`), &seed); err != nil {
		t.Fatal(err)
	}
	if err := r.ActivateIssuer(context.Background(), seed.Issuer, nil); err != nil {
		t.Fatal(err)
	}
	return r, b, seed
}

func volumeSpec(name string) VolumeSpec {
	return VolumeSpec{CSIName: name, Fingerprint: strings.Repeat("1", 64), RequiredBytes: 10 << 30, FSType: "ext4", AccessMode: "SINGLE_NODE_WRITER"}
}

func prepared(t *testing.T) (*Repository, *MemoryBackend, OperationRecord) {
	t.Helper()
	ctx := context.Background()
	r, b, seed := fresh(t)
	v, err := r.CreateVolume(ctx, volumeSpec("pvc-a"))
	if err != nil {
		t.Fatal(err)
	}
	v.Status.ProviderID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
	v.Status.SizeGB = 10
	v, err = r.SaveVolume(ctx, v, v.Version)
	if err != nil {
		t.Fatal(err)
	}
	var ns NodeSpec
	if err := json.Unmarshal([]byte(`{"ref":{"name":"worker-a","uid":"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb","projectID":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","serverID":"cccccccc-cccc-4ccc-8ccc-cccccccccccc","epoch":1},"identity":{"schemaVersion":1,"projectID":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","providerName":"clo-worker-a-attempt","attempt":"attempt"}}`), &ns); err != nil {
		t.Fatal(err)
	}
	n, err := r.CreateNode(ctx, ns)
	if err != nil {
		t.Fatal(err)
	}
	n.Status.RootSerial = "root-serial"
	n.Status.BootVolumeIDs = []string{"boot-volume"}
	n, err = r.SaveNode(ctx, n, n.Version)
	if err != nil {
		t.Fatal(err)
	}
	seed.VolumeRecordID = v.Name
	seed.NodeBindingID = n.Name
	seed.Issuer.BindingID = n.Name
	// The actor's trusted host binding is fixed before this first operation.
	current, err := r.Issuer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	current.Status.Current.BindingID = n.Name
	if _, err = r.SaveIssuer(ctx, current, current.Version); err != nil {
		t.Fatal(err)
	}
	seed.Deadline = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	op, err := r.CreateOperation(ctx, seed)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.LockNode(ctx, n.Name, op.Name, n.Version); err != nil {
		t.Fatal(err)
	}
	if err = r.LockVolume(ctx, v.Name, op.Name, v.Version); err != nil {
		t.Fatal(err)
	}
	if err = r.AuditOwned(ctx, []ProviderVolume{{ID: v.Status.ProviderID, Name: v.Spec.ProviderName}}); err != nil {
		t.Fatal(err)
	}
	return r, b, op
}

func TestRegistryCASHasSingleWinner(t *testing.T) {
	ctx := context.Background()
	b := NewMemoryBackend()
	d, err := b.Create(ctx, Document{Kind: KindVolume, Name: "one", Spec: json.RawMessage(`{"owner":"a"}`), Status: json.RawMessage(`{"pending":""}`)})
	if err != nil {
		t.Fatal(err)
	}
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			copy := d
			copy.Status = json.RawMessage(`{"pending":"claimed"}`)
			if _, e := b.CAS(ctx, copy, d.Version); e == nil {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("CAS winners=%d", wins.Load())
	}
	got, err := b.Get(ctx, KindVolume, "one")
	if err != nil {
		t.Fatal(err)
	}
	got.Spec[2] = 'X'
	again, err := b.Get(ctx, KindVolume, "one")
	if err != nil || string(again.Spec) != `{"owner":"a"}` {
		t.Fatalf("store shared mutable state: %s %v", again.Spec, err)
	}
}

func TestDispatchPermissionIsSingleUse(t *testing.T) {
	r, _, op := prepared(t)
	ctx := context.Background()
	var calls atomic.Int32
	var grants atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g, e := r.Dispatch(ctx, op.Name, op.Spec.Issuer)
			if e != nil {
				return
			}
			grants.Add(1)
			send := func() error { calls.Add(1); return nil }
			if err := g.Send(op.Spec.Issuer, send); err != nil {
				t.Error(err)
			}
			if err := g.Send(op.Spec.Issuer, send); err == nil {
				t.Error("grant reused")
			}
		}()
	}
	wg.Wait()
	if grants.Load() != 1 || calls.Load() != 1 {
		t.Fatalf("grants=%d sends=%d", grants.Load(), calls.Load())
	}
	saved, err := r.Operation(ctx, op.Name)
	if err != nil || saved.Status.Phase != Dispatched {
		t.Fatalf("dispatch not durable: %+v %v", saved, err)
	}
}

func TestLeaseExpiryDoesNotReleaseDispatchedLock(t *testing.T) {
	r, _, op := prepared(t)
	ctx := context.Background()
	if _, err := r.Dispatch(ctx, op.Name, op.Spec.Issuer); err != nil {
		t.Fatal(err)
	}
	next := op.Spec.Issuer
	next.ProcessNonce = "replacement-process"
	next.HostPID = 71
	if err := r.ActivateIssuer(ctx, next, nil); err == nil {
		t.Fatal("new issuer replaced live/unknown process")
	}
	if _, err := r.Dispatch(ctx, op.Name, next); err == nil {
		t.Fatal("expired operation re-dispatched by replacement")
	}
	n, err := r.Node(ctx, op.Spec.NodeBindingID)
	if err != nil || n.Status.PendingEnrollment != op.Name {
		t.Fatalf("node lock lost: %+v %v", n, err)
	}
	v, err := r.Volume(ctx, op.Spec.VolumeRecordID)
	if err != nil || v.Status.PendingOp != op.Name {
		t.Fatalf("volume lock lost: %+v %v", v, err)
	}
}

func TestIdentityAndFingerprintImmutable(t *testing.T) {
	r, _, _ := fresh(t)
	ctx := context.Background()
	v, err := r.CreateVolume(ctx, volumeSpec("pvc-a"))
	if err != nil {
		t.Fatal(err)
	}
	v.Status.ProviderID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
	v.Status.Serial = "serial-a"
	v, err = r.SaveVolume(ctx, v, v.Version)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*VolumeRecord){
		func(x *VolumeRecord) { x.Spec.Fingerprint = "other" },
		func(x *VolumeRecord) { x.Spec.InstallationID = "other" },
		func(x *VolumeRecord) { x.Status.ProviderID = "foreign-volume" },
		func(x *VolumeRecord) { x.Status.Serial = "foreign-serial" },
	} {
		copy := v
		change(&copy)
		if _, e := r.SaveVolume(ctx, copy, v.Version); e == nil {
			t.Fatal("immutable ownership changed")
		}
	}
	got, err := r.Volume(ctx, v.Name)
	if err != nil || got.Status.Serial != "serial-a" || got.Spec.Fingerprint != strings.Repeat("1", 64) {
		t.Fatalf("failed update changed data: %+v %v", got, err)
	}
}

func TestExportRestorePreservesSpecStatusAndInstallation(t *testing.T) {
	r, _, _ := fresh(t)
	ctx := context.Background()
	spec := volumeSpec("pvc-a")
	spec.RequiredBytes = (1 << 54) + 3
	v, err := r.CreateVolume(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	v.Status.ProviderID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
	v.Status.Serial = "serial-a"
	v, err = r.SaveVolume(ctx, v, v.Version)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.Export(ctx); err == nil {
		t.Fatal("export while writers unfrozen")
	}
	if err = r.SetFrozen(ctx, true); err != nil {
		t.Fatal(err)
	}
	blob, err := r.Export(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r2 := New(NewMemoryBackend(), Installation{ID: installID, ProjectID: projectID})
	if err = r2.SetFrozen(ctx, true); err != nil {
		t.Fatal(err)
	}
	if err = r2.Restore(ctx, blob); err != nil {
		t.Fatal(err)
	}
	got, err := r2.Volume(ctx, v.Name)
	if err != nil || got.Spec.RequiredBytes != (1<<54)+3 || got.Status.Serial != "serial-a" || got.Status.ProviderID != v.Status.ProviderID {
		t.Fatalf("restore lost state/int64: %+v %v", got, err)
	}
	wrong := New(NewMemoryBackend(), Installation{ID: installID, ProjectID: "other-project"})
	_ = wrong.SetFrozen(ctx, true)
	if err = wrong.Restore(ctx, blob); err == nil {
		t.Fatal("wrong-project backup restored")
	}
	corrupt := append([]byte(nil), blob...)
	corrupt[len(corrupt)/2] ^= 1
	if err = r2.Restore(ctx, corrupt); err == nil {
		t.Fatal("corrupted backup accepted")
	}
}

func TestMissingRegistryWithOwnedPrefixBlocksMutations(t *testing.T) {
	r, _, seed := fresh(t)
	ctx := context.Background()
	v, err := r.CreateVolume(ctx, volumeSpec("pvc-a"))
	if err != nil {
		t.Fatal(err)
	}
	unknownName := "csi-11111111111141118111111111111111-abcdefabcdefabcdefabcdef"
	if err = r.AuditOwned(ctx, []ProviderVolume{{ID: "unknown", Name: unknownName}}); err == nil {
		t.Fatal("lost ownership registry ignored")
	}
	seed.Type = "create"
	seed.VolumeRecordID = v.Name
	seed.NodeBindingID = ""
	op, err := r.CreateOperation(ctx, seed)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.LockVolume(ctx, v.Name, op.Name, v.Version); err != nil {
		t.Fatal(err)
	}
	if _, err = r.Dispatch(ctx, op.Name, op.Spec.Issuer); err == nil {
		t.Fatal("mutation authorized after failed inventory audit")
	}
}

func TestHashCollisionCannotAdoptOtherOwner(t *testing.T) {
	r, b, _ := fresh(t)
	ctx := context.Background()
	v, err := r.CreateVolume(ctx, volumeSpec("pvc-a"))
	if err != nil {
		t.Fatal(err)
	}
	conflicting := v.Spec
	conflicting.CSIName = "pvc-b"
	conflicting.Fingerprint = strings.Repeat("2", 64)
	spec, _ := json.Marshal(conflicting)
	if _, err = b.Create(ctx, Document{Kind: KindVolume, Name: "other-owner", Spec: spec, Status: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err = r.AuditOwned(ctx, []ProviderVolume{{ID: "v", Name: v.Spec.ProviderName}}); err == nil {
		t.Fatal("duplicate provider name adopted")
	}
}
