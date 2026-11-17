package node

import (
	"context"
	"errors"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"sync/atomic"
	"testing"
)

type statsInspectorTrap struct {
	Inspector
	calls atomic.Int32
}

func (i *statsInspectorTrap) Snapshot(context.Context) (protocol.Snapshot, error) {
	i.calls.Add(1)
	return protocol.Snapshot{}, errors.New("unowned stats must not inspect host devices")
}

type statsFilesystemTrap struct {
	Filesystem
	calls atomic.Int32
}

func (f *statsFilesystemTrap) Stats(context.Context, string) (Usage, error) {
	f.calls.Add(1)
	return Usage{}, errors.New("unowned stats must not access caller filesystem path")
}

func TestStatsMissingVolumeOrUnownedPathReturnsNotFoundWithoutHostAccess(t *testing.T) {
	h := newFS(t, true)
	h.stageReady(t)
	i := &statsInspectorTrap{Inspector: h.ins}
	f := &statsFilesystemTrap{Filesystem: h.fs}
	h.r.Inspector, h.r.Filesystem = i, f
	c, _ := nodeRPC(t, h, &nodeAuth{p: h.p})
	missing := "clo:11111111-1111-4111-8111-111111111111:ffffffff-ffff-4fff-8fff-ffffffffffff"
	for _, tt := range []struct {
		handle, path string
		want         codes.Code
	}{
		{missing, "some/path", codes.NotFound},
		{fsHandle, "some/path", codes.NotFound},
		{fsHandle, "/outside", codes.NotFound},
		{fsHandle, "", codes.InvalidArgument},
		{missing, "", codes.InvalidArgument},
	} {
		_, e := c.NodeGetVolumeStats(context.Background(), &csi.NodeGetVolumeStatsRequest{VolumeId: tt.handle, VolumePath: tt.path})
		if status.Code(e) != tt.want {
			t.Errorf("stats(%q,%q): want %s, got %v", tt.handle, tt.path, tt.want, e)
		}
	}
	if i.calls.Load() != 0 || f.calls.Load() != 0 {
		t.Fatal("unowned stats reached host inspection or statfs")
	}
}

func TestStatsOwnedPathStillRejectsSymlinkBeforeHostAccess(t *testing.T) {
	h := newFS(t, true)
	h.stageReady(t)
	if e := os.Remove(h.stage); e != nil {
		t.Fatal(e)
	}
	if e := os.Symlink(t.TempDir(), h.stage); e != nil {
		t.Fatal(e)
	}
	i := &statsInspectorTrap{Inspector: h.ins}
	f := &statsFilesystemTrap{Filesystem: h.fs}
	h.r.Inspector, h.r.Filesystem = i, f
	if _, e := h.r.Stats(context.Background(), fsHandle, h.stage); e == nil {
		t.Fatal("owned-path symlink was followed")
	}
	if i.calls.Load() != 0 || f.calls.Load() != 0 {
		t.Fatal("symlink reached host inspection or statfs")
	}
}
