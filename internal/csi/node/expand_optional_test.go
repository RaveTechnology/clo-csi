package node

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNodeExpandMissingVolumeWithoutOptionalRangeDoesNotInspectHost(t *testing.T) {
	h := newFS(t, true)
	h.stageReady(t)
	i := &statsInspectorTrap{Inspector: h.ins}
	h.r.Inspector = i
	c, _ := nodeRPC(t, h, &nodeAuth{p: h.p})
	for _, tt := range []struct {
		handle, path string
		want         codes.Code
	}{
		{"clo:11111111-1111-4111-8111-111111111111:ffffffff-ffff-4fff-8fff-ffffffffffff", "some/path", codes.NotFound},
		{fsHandle, "some/path", codes.NotFound},
		{fsHandle, "", codes.InvalidArgument},
	} {
		_, e := c.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{VolumeId: tt.handle, VolumePath: tt.path})
		if status.Code(e) != tt.want {
			t.Errorf("expand(%q,%q): want %s, got %v", tt.handle, tt.path, tt.want, e)
		}
	}
	if i.calls.Load() != 0 || h.fs.expands != 0 {
		t.Fatal("unowned expansion inspected or mutated host")
	}
}

func expansionHost(t *testing.T) (*fsHarness, *filesystemRunner) {
	t.Helper()
	h := newFS(t, true)
	h.stageReady(t)
	runner := &filesystemRunner{formatted: true}
	h.r.Filesystem = &HostFilesystem{Runner: runner, Inspector: h.ins,
		guard:       func(Device, bool) error { return nil },
		writeRescan: func(string, []byte) error { return nil },
	}
	return h, runner
}

func TestNodeExpandOptionalRangeGrowsOwnedFilesystemToActualBlockCapacity(t *testing.T) {
	for _, capacity := range []*csi.CapacityRange{nil, {}} {
		h, runner := expansionHost(t)
		c, _ := nodeRPC(t, h, &nodeAuth{p: h.p})
		result, e := c.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{VolumeId: fsHandle, VolumePath: h.stage, CapacityRange: capacity})
		if e != nil || result.GetCapacityBytes() != 12884905984 || !runner.resized {
			t.Errorf("optional range: actual FS growth required, result=%v error=%v resized=%v", result, e, runner.resized)
		}
	}
}

func TestNodeExpandUpperBoundRejectsPhysicalOversizeBeforeFilesystemGrowth(t *testing.T) {
	h, runner := expansionHost(t)
	c, _ := nodeRPC(t, h, &nodeAuth{p: h.p})
	_, e := c.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{VolumeId: fsHandle, VolumePath: h.stage,
		CapacityRange: &csi.CapacityRange{RequiredBytes: 11 << 30, LimitBytes: 12 << 30}})
	if status.Code(e) != codes.InvalidArgument || runner.resized {
		t.Fatalf("physical oversize must be refused before resize, error=%v resized=%v", e, runner.resized)
	}
}

type undersizedBlockRunner struct{ *filesystemRunner }

func (r undersizedBlockRunner) Run(ctx context.Context, binary string, args ...string) ([]byte, error) {
	if binary == "/usr/sbin/blockdev" {
		return []byte("8589934592\n"), nil
	}
	return r.filesystemRunner.Run(ctx, binary, args...)
}

func TestNodeExpandRefusesFilesystemLargerThanActualDeviceBeforeResize(t *testing.T) {
	h, runner := expansionHost(t)
	h.r.Filesystem.(*HostFilesystem).Runner = undersizedBlockRunner{runner}
	c, _ := nodeRPC(t, h, &nodeAuth{p: h.p})
	_, e := c.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{VolumeId: fsHandle, VolumePath: h.stage})
	if e == nil || runner.resized {
		t.Fatalf("10GiB filesystem on an actual 8GiB device must stop before resize2fs, error=%v resized=%v", e, runner.resized)
	}
}

func TestNodeExpandAcceptsPublishedReadWriteTargetByMapMembership(t *testing.T) {
	h := newFS(t, true)
	h.stageReady(t)
	if e := h.r.Publish(context.Background(), h.p, h.stage, h.targetA, false); e != nil {
		t.Fatal(e)
	}
	c, _ := nodeRPC(t, h, &nodeAuth{p: h.p})
	result, e := c.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumeId: fsHandle, VolumePath: h.targetA, CapacityRange: &csi.CapacityRange{RequiredBytes: 12 << 30},
	})
	if e != nil || result.GetCapacityBytes() < 12<<30 || h.fs.expands != 1 {
		t.Fatalf("RW target has a present false value; it is owned: result=%v error=%v expands=%d", result, e, h.fs.expands)
	}
}
