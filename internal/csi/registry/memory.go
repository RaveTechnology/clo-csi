package registry

import (
	"bytes"
	"context"
	"fmt"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/google/uuid"
	"sync"
)

type MemoryBackend struct {
	mu   sync.Mutex
	seq  uint64
	docs map[string]Document
}

func NewMemoryBackend() *MemoryBackend { return &MemoryBackend{docs: map[string]Document{}} }
func (b *MemoryBackend) Get(ctx context.Context, kind, name string) (Document, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Document{}, err
	}
	d, ok := b.docs[kind+"/"+name]
	if !ok {
		return d, protocol.Fail(protocol.NotFound, "record missing")
	}
	return clone(d), nil
}
func (b *MemoryBackend) Create(ctx context.Context, d Document) (Document, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Document{}, err
	}
	key := d.Kind + "/" + d.Name
	if _, ok := b.docs[key]; ok {
		return Document{}, protocol.Fail(protocol.Conflict, "record exists")
	}
	b.seq++
	d.UID = uuid.NewString()
	d.Version = fmt.Sprint(b.seq)
	b.docs[key] = clone(d)
	return clone(d), nil
}
func (b *MemoryBackend) CAS(ctx context.Context, d Document, version string) (Document, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Document{}, err
	}
	key := d.Kind + "/" + d.Name
	old, ok := b.docs[key]
	if !ok {
		return Document{}, protocol.Fail(protocol.NotFound, "record missing")
	}
	if old.Version != version || old.UID != d.UID {
		return Document{}, protocol.Fail(protocol.Conflict, "stale record version")
	}
	if !bytes.Equal(old.Spec, d.Spec) {
		return Document{}, protocol.Fail(protocol.Invalid, "immutable spec")
	}
	b.seq++
	d.Version = fmt.Sprint(b.seq)
	b.docs[key] = clone(d)
	return clone(d), nil
}
func (b *MemoryBackend) List(ctx context.Context, kind string) ([]Document, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out []Document
	for _, d := range b.docs {
		if d.Kind == kind {
			out = append(out, clone(d))
		}
	}
	return out, nil
}
