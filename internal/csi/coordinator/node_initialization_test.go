package coordinator

import (
	"context"
	"fmt"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"testing"
)

type lostNodeCreate struct {
	registry.Backend
	fail bool
}

func (b *lostNodeCreate) Create(ctx context.Context, d registry.Document) (registry.Document, error) {
	out, e := b.Backend.Create(ctx, d)
	if e == nil && d.Kind == registry.KindNode && b.fail {
		b.fail = false
		return registry.Document{}, fmt.Errorf("injected lost CreateNode response")
	}
	return out, e
}
func TestRegisterNodeRecoversLostCreateResponseWithCompleteRootIdentity(t *testing.T) {
	seed := setup(t, false)
	b := &lostNodeCreate{registry.NewMemoryBackend(), true}
	r := registry.New(b, registry.Installation{ID: installationID, ProjectID: projectID})
	c := New(r, seed.api, seed.channel, seed.cfg)
	if _, e := c.RegisterNode(context.Background(), seed.caller, seed.hello); e == nil {
		t.Fatal("fault not injected")
	}
	restarted := New(registry.New(b, r.Installation()), seed.api, seed.channel, seed.cfg)
	ref, e := restarted.RegisterNode(context.Background(), seed.caller, seed.hello)
	if e != nil {
		t.Fatalf("restart cannot recover persisted node: %v", e)
	}
	n, e := r.Node(context.Background(), ref.BindingID)
	if e != nil || n.Status.RootSerial == "" || n.Status.BootID != seed.hello.Snapshot.BootID || len(n.Status.BootVolumeIDs) != 1 {
		t.Fatalf("incomplete node identity: %+v %v", n.Status, e)
	}
}
