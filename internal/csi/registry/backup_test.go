package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
)

func TestRestoreIgnoresJSONKeyOrderWithoutLosingInt64(t *testing.T) {
	ctx := context.Background()
	r, _, _ := fresh(t)
	s := volumeSpec("pvc-a")
	s.RequiredBytes = (1 << 54) + 3
	v, e := r.CreateVolume(ctx, s)
	if e != nil {
		t.Fatal(e)
	}
	if e = r.SetFrozen(ctx, true); e != nil {
		t.Fatal(e)
	}
	blob, e := r.Export(ctx)
	if e != nil {
		t.Fatal(e)
	}
	var env backupEnvelope
	_ = json.Unmarshal(blob, &env)
	var data backupData
	_ = json.Unmarshal(env.Data, &data)
	b := NewMemoryBackend()
	r2 := New(b, r.installation)
	_ = r2.SetFrozen(ctx, true)
	for _, d := range data.Documents {
		if d.Kind != KindVolume {
			continue
		}
		var spec map[string]json.RawMessage
		_ = json.Unmarshal(d.Spec, &spec)
		d.Spec, _ = json.Marshal(spec)
		if _, e = b.Create(ctx, d); e != nil {
			t.Fatal(e)
		}
	}
	if e = r2.Restore(ctx, blob); e != nil {
		t.Fatal(e)
	}
	got, e := r2.Volume(ctx, v.Name)
	if e != nil || got.Spec.RequiredBytes != s.RequiredBytes {
		t.Fatal("canonical restore lost integer")
	}
}
func TestRestoreRejectsForeignFenceBeforeAnyWrites(t *testing.T) {
	ctx := context.Background()
	r, _, _ := fresh(t)
	_, _ = r.CreateVolume(ctx, volumeSpec("pvc-a"))
	_ = r.SetFrozen(ctx, true)
	blob, e := r.Export(ctx)
	if e != nil {
		t.Fatal(e)
	}
	var env backupEnvelope
	_ = json.Unmarshal(blob, &env)
	var data backupData
	_ = json.Unmarshal(env.Data, &data)
	data.Documents = append(data.Documents, document(KindFence, Meta{Name: "bad-fence"}, FenceSpec{InstallationID: installID, ProjectID: "other-project"}, FenceStatus{Initialized: true}))
	raw, _ := json.Marshal(data)
	h := sha256.Sum256(raw)
	blob, _ = json.Marshal(backupEnvelope{raw, hex.EncodeToString(h[:])})
	b := NewMemoryBackend()
	r2 := New(b, r.installation)
	_ = r2.SetFrozen(ctx, true)
	if e = r2.Restore(ctx, blob); e == nil {
		t.Fatal("foreign fence accepted")
	}
	ds, _ := b.List(ctx, KindVolume)
	if len(ds) != 0 {
		t.Fatal("partial writes before scope validation")
	}
}
