package node

import (
	"context"
	"errors"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"math"
	"reflect"
	"strings"
	"testing"
)

type commandCall struct {
	binary string
	args   []string
}
type filesystemRunner struct {
	calls                          []commandCall
	formatted, resized, failResize bool
	probeOutput                    string
}

func (r *filesystemRunner) Run(_ context.Context, binary string, args ...string) ([]byte, error) {
	r.calls = append(r.calls, commandCall{binary, append([]string(nil), args...)})
	switch binary {
	case "/usr/sbin/wipefs":
		if r.probeOutput != "" {
			return []byte(r.probeOutput), nil
		}
		if r.formatted {
			return []byte(`{"signatures":[{"type":"ext4","uuid":"` + fsVolumeUUID + `"}]}`), nil
		}
		return []byte(`{"signatures":[]}`), nil
	case "/usr/sbin/mkfs.ext4":
		r.formatted = true
	case "/usr/bin/stat":
		return []byte("4096 100 60 70 1000 800\n"), nil
	case "/usr/sbin/blockdev":
		return []byte("12884905984\n"), nil
	case "/usr/sbin/dumpe2fs":
		if r.resized {
			return []byte("Block count: 3145729\nBlock size: 4096\n"), nil
		}
		return []byte("Block count: 2621440\nBlock size: 4096\n"), nil
	case "/usr/sbin/resize2fs":
		if r.failResize {
			return nil, errors.New("resize failed")
		}
		r.resized = true
	}
	return nil, nil
}
func TestHostFilesystemFixedCommandsAndNoAutomaticReformat(t *testing.T) {
	h := newFS(t, false)
	runner := &filesystemRunner{}
	fs := &HostFilesystem{Runner: runner, Inspector: h.ins, guard: func(Device, bool) error { return nil }}
	d, e := h.ins.BySerial(context.Background(), h.p.Volume.Serial)
	if e != nil {
		t.Fatal(e)
	}
	d.FSUUID = fsVolumeUUID
	ctx := context.Background()
	if e = fs.Format(ctx, d, fsVolumeUUID); e != nil {
		t.Fatal(e)
	}
	if e = fs.Format(ctx, d, fsVolumeUUID); e == nil {
		t.Fatal("host formatter accepted an existing signature")
	}
	if e = fs.Check(ctx, d); e != nil {
		t.Fatal(e)
	}
	if e = fs.Mount(ctx, d, h.stage); e != nil {
		t.Fatal(e)
	}
	if e = fs.Bind(ctx, h.stage, h.targetA, true); e != nil {
		t.Fatal(e)
	}
	if e = fs.Unmount(ctx, h.targetA); e != nil {
		t.Fatal(e)
	}
	counts := map[string]int{}
	for _, c := range runner.calls {
		counts[c.binary]++
		switch c.binary {
		case "/usr/sbin/mkfs.ext4":
			want := []string{"-q", "-U", fsVolumeUUID, "-E", "lazy_itable_init=0,lazy_journal_init=0", "/dev/sdc"}
			if !reflect.DeepEqual(c.args, want) {
				t.Fatalf("mkfs args %v", c.args)
			}
		case "/usr/sbin/e2fsck":
			if !reflect.DeepEqual(c.args, []string{"-f", "-n", "/dev/sdc"}) {
				t.Fatal("write/force fsck", c.args)
			}
		case "/usr/sbin/wipefs":
			if !reflect.DeepEqual(c.args, []string{"--json", "--no-act", "/dev/sdc"}) {
				t.Fatal("mutating probe", c.args)
			}
		case "/usr/bin/mount", "/usr/bin/umount":
		default:
			t.Fatal("unexpected executable", c.binary)
		}
	}
	if counts["/usr/sbin/mkfs.ext4"] != 1 || counts["/usr/bin/mount"] != 3 {
		t.Fatalf("command counts %v", counts)
	}
	usage, e := fs.Stats(ctx, h.stage)
	if e != nil || usage.TotalBytes != 409600 || usage.UsedBytes != 122880 || usage.AvailableBytes != 245760 || usage.UsedInodes != 200 {
		t.Fatalf("statfs arithmetic %+v %v", usage, e)
	}
}
func TestHostFilesystemResizeChecksActualFilesystem(t *testing.T) {
	for _, fail := range []bool{false, true} {
		h := newFS(t, true)
		runner := &filesystemRunner{formatted: true, failResize: fail}
		writes := 0
		fs := &HostFilesystem{Runner: runner, Inspector: h.ins, guard: func(Device, bool) error { return nil }, writeRescan: func(path string, data []byte) error {
			if path != "/proc/1/root/sys/class/block/sdc/device/rescan" || string(data) != "1\n" {
				t.Error("wrong rescan path/body")
			}
			writes++
			return nil
		}}
		d, e := h.ins.BySerial(context.Background(), h.p.Volume.Serial)
		if e != nil {
			t.Fatal(e)
		}
		size, e := fs.Expand(context.Background(), d, 12<<30, 0)
		if fail && e == nil {
			t.Fatal("failed resize reported success")
		}
		if !fail && (e != nil || size < 12<<30) {
			t.Fatalf("resize result %d %v", size, e)
		}
		if writes != 1 {
			t.Fatal("rescan missing")
		}
		for _, c := range runner.calls {
			if strings.Contains(c.binary, "sh") {
				t.Fatal("shell interpolation")
			}
			if c.binary == "/usr/sbin/resize2fs" && !reflect.DeepEqual(c.args, []string{"/dev/sdc"}) {
				t.Fatal("wrong resize target", c.args)
			}
		}
	}
}
func TestHostFilesystemRootGuardPrecedesCommands(t *testing.T) {
	h := newFS(t, true)
	runner := &filesystemRunner{}
	fs := &HostFilesystem{Runner: runner, Inspector: h.ins, guard: func(Device, bool) error { return nil }}
	root := Device{h.ins.s.Disks[0]}
	if e := fs.Format(context.Background(), root, fsVolumeUUID); e == nil {
		t.Fatal("root format allowed")
	}
	if len(runner.calls) != 0 {
		t.Fatal("root reached host command")
	}
}

func TestSignatureProbeRequiresCompleteInventory(t *testing.T) {
	for _, raw := range []string{`{}`, `{"signatures":null}`} {
		h := newFS(t, false)
		runner := &filesystemRunner{probeOutput: raw}
		fs := &HostFilesystem{Runner: runner, Inspector: h.ins, guard: func(Device, bool) error { return nil }}
		d, e := h.ins.BySerial(context.Background(), h.p.Volume.Serial)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = fs.Probe(context.Background(), d); e == nil {
			t.Fatal("missing signature inventory treated as a blank disk")
		}
	}
}
func TestBlockGuardRejectsNonBlockDevice(t *testing.T) {
	if e := blockGuard(Device{Disk: protocol.Disk{Path: "/dev/null", MajorMinor: "1:3"}}, false); e == nil {
		t.Fatal("character device accepted as disk")
	}
}

func TestHostFilesystemExpansionOverflowPrecedesRescan(t *testing.T) {
	h := newFS(t, true)
	runner := &filesystemRunner{formatted: true}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writes := 0
	fs := &HostFilesystem{Runner: runner, Inspector: h.ins, guard: func(Device, bool) error { return nil }, writeRescan: func(string, []byte) error { writes++; cancel(); return nil }}
	d, e := h.ins.BySerial(ctx, h.p.Volume.Serial)
	if e != nil {
		t.Fatal(e)
	}
	_, e = fs.Expand(ctx, d, math.MaxInt64, 0)
	if e == nil || writes != 0 || len(runner.calls) != 0 {
		t.Fatal("overflow reached host commands/rescan")
	}
}
