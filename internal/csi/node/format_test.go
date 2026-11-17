package node

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestStageFindsSerialNotApiPath(t *testing.T) {
	h := newFS(t, false)
	h.stageReady(t)
	if h.fs.formats != 1 || h.fs.lastDevice != "/dev/sdc" || h.fs.lastUUID != fsVolumeUUID {
		t.Fatal("wrong device/UUID or format count")
	}
}
func TestStageRejectsRootForeignSignatureAndSymlinkEscape(t *testing.T) {
	for _, which := range []string{"root", "signature", "symlink"} {
		t.Run(which, func(t *testing.T) {
			h := newFS(t, false)
			switch which {
			case "root":
				h.p.Volume.Serial = "root-serial"
			case "signature":
				h.ins.s.Disks[1].FSType = "xfs"
				h.ins.s.Disks[1].FSUUID = "foreign"
			case "symlink":
				if e := os.Symlink(t.TempDir(), filepath.Join(h.root, "plugins")); e != nil {
					t.Fatal(e)
				}
			}
			if _, e := h.r.Stage(context.Background(), h.p, h.stage); e == nil {
				t.Fatal("unsafe stage succeeded")
			}
			if h.fs.formats != 0 || h.fs.mounts != 0 {
				t.Fatal("unsafe device changed")
			}
		})
	}
}
func TestFormatOnceAcrossCrashAndPermitReplay(t *testing.T) {
	h := newFS(t, false)
	h.fs.formatFailure = "after-write"
	if _, e := h.r.Stage(context.Background(), h.p, h.stage); e == nil {
		t.Fatal("crash missed")
	}
	j, e := NewJournal(h.j.root)
	if e != nil {
		t.Fatal(e)
	}
	h.r.Journal = j
	h.fs.formatFailure = ""
	result, e := h.r.Stage(context.Background(), h.p, h.stage)
	if e != nil || !result.NeedsReport {
		t.Fatal("own FS not recovered read-only", e)
	}
	if _, e = h.r.Stage(context.Background(), h.p, h.stage); e != nil {
		t.Fatal(e)
	}
	if h.fs.formats != 1 || h.fs.checks == 0 {
		t.Fatal("format repeated or recovery check skipped")
	}
}
func TestExistingOwnedExt4NeverReformatted(t *testing.T) {
	h := newFS(t, true)
	h.stageReady(t)
	h.stageReady(t)
	if h.fs.formats != 0 || h.fs.mounts != 1 {
		t.Fatal("initialized FS reformatted or mounted twice")
	}
}
func TestIncompleteFormatRequiresRecovery(t *testing.T) {
	h := newFS(t, false)
	h.fs.formatFailure = "partial"
	_, _ = h.r.Stage(context.Background(), h.p, h.stage)
	h.fs.formatFailure = ""
	if _, e := h.r.Stage(context.Background(), h.p, h.stage); e == nil {
		t.Fatal("partial format reused")
	}
	if h.fs.formats != 1 || h.fs.mounts != 0 {
		t.Fatal("partial FS rewritten or mounted")
	}
}

func TestConflictingCompletedFormatPermitCannotWrite(t *testing.T) {
	h := newFS(t, false)
	h.p.Initialization = "FormatComplete"
	h.p.AllowFormat = true
	if _, e := h.r.Stage(context.Background(), h.p, h.stage); e == nil {
		t.Fatal("completed filesystem was given a new mkfs")
	}
	if h.fs.formats != 0 {
		t.Fatal("conflicting permit reached mkfs")
	}
}
