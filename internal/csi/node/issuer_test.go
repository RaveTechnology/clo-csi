package node

import (
	"context"
	"fmt"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const podUID = "99999999-9999-4999-8999-999999999999"

func procFixture(t *testing.T) (*Verifier, string, protocol.NodeRef) {
	t.Helper()
	root := t.TempDir()
	if e := os.MkdirAll(filepath.Join(root, "sys/kernel/random"), 0700); e != nil {
		t.Fatal(e)
	}
	_ = os.WriteFile(filepath.Join(root, "sys/kernel/random/boot_id"), []byte("boot-a\n"), 0600)
	n := protocol.NodeRef{Name: "worker", UID: "node-a", BindingID: "binding-a", ServerID: "server-a", Epoch: 1}
	v := &Verifier{ProcRoot: root, Node: func() protocol.NodeRef { return n }}
	writeProc(t, root, 70, 1000, "T")
	return v, root, n
}
func writeProc(t *testing.T, root string, pid int, ticks uint64, state string) {
	t.Helper()
	dir := filepath.Join(root, fmt.Sprint(pid))
	_ = os.MkdirAll(dir, 0700)
	fields := make([]string, 50)
	for i := range fields {
		fields[i] = "0"
	}
	fields[0] = state
	fields[19] = fmt.Sprint(ticks)
	_ = os.WriteFile(filepath.Join(dir, "stat"), []byte(fmt.Sprintf("%d (controller ) odd) %s", pid, strings.Join(fields, " "))), 0600)
	_ = os.WriteFile(filepath.Join(dir, "cmdline"), []byte("clo-csi\x00controller\x00--process-nonce=nonce-a\x00"), 0600)
	_ = os.WriteFile(filepath.Join(dir, "cgroup"), []byte("0::/kubepods.slice/kubepods-burstable-pod"+strings.ReplaceAll(podUID, "-", "_")+".slice/cri-containerd.scope\n"), 0600)
}
func descriptor(t *testing.T, v *Verifier) protocol.IssuerDescriptor {
	t.Helper()
	d, e := v.Locate(context.Background(), podUID, "nonce-a")
	if e != nil {
		t.Fatal(e)
	}
	if d.HostPID != 70 || d.StartTicks != 1000 {
		t.Fatalf("wrong descriptor: %+v", d)
	}
	return d
}
func TestIssuerPIDReuseAndBootChange(t *testing.T) {
	v, root, _ := procFixture(t)
	d := descriptor(t, v)
	writeProc(t, root, 70, 2000, "S")
	p, e := v.Verify(context.Background(), d)
	if e != nil || p.Verdict != protocol.Gone {
		t.Fatalf("reuse not gone: %+v %v", p, e)
	}
	writeProc(t, root, 70, 1000, "T")
	_ = os.WriteFile(filepath.Join(root, "sys/kernel/random/boot_id"), []byte("boot-b"), 0600)
	p, e = v.Verify(context.Background(), d)
	if e != nil || p.Verdict != protocol.Gone {
		t.Fatalf("reboot not gone: %+v %v", p, e)
	}
}
func TestIssuerReplacementHostIsUnknown(t *testing.T) {
	v, _, n := procFixture(t)
	d := descriptor(t, v)
	n.ServerID = "replacement"
	v.Node = func() protocol.NodeRef { return n }
	p, e := v.Verify(context.Background(), d)
	if e != nil || p.Verdict != protocol.Unknown {
		t.Fatalf("replacement proved termination: %+v %v", p, e)
	}
}
func TestProcPermissionErrorIsNotGone(t *testing.T) {
	v, _, _ := procFixture(t)
	d := descriptor(t, v)
	v.ReadFile = func(path string) ([]byte, error) {
		if strings.HasSuffix(path, "/70/stat") {
			return nil, fs.ErrPermission
		}
		return os.ReadFile(path)
	}
	p, e := v.Verify(context.Background(), d)
	if e != nil || p.Verdict != protocol.Unknown {
		t.Fatalf("permission inferred gone: %+v %v", p, e)
	}
}
func TestPausedIssuerRemainsAlive(t *testing.T) {
	v, root, _ := procFixture(t)
	d := descriptor(t, v)
	p, e := v.Verify(context.Background(), d)
	if e != nil || p.Verdict != protocol.Alive {
		t.Fatalf("paused process not alive: %+v %v", p, e)
	}
	_ = os.Remove(filepath.Join(root, "70/stat"))
	p, e = v.Verify(context.Background(), d)
	if e != nil || p.Verdict != protocol.Gone {
		t.Fatalf("missing process not gone: %+v %v", p, e)
	}
}

func TestListIssuersFindsAllScopedHostProcesses(t *testing.T) {
	v, root, n := procFixture(t)
	n.ProjectID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	v.Node = func() protocol.NodeRef { return n }
	install := "11111111-1111-4111-8111-111111111111"
	writeProc(t, root, 71, 2000, "T")
	for _, pid := range []int{70, 71} {
		nonce := "nonce-a"
		if pid == 71 {
			nonce = "nonce-b"
		}
		cmd := []byte("clo-csi\x00controller\x00--project-id=" + n.ProjectID + "\x00--installation-id=" + install + "\x00--process-nonce=" + nonce + "\x00")
		if e := os.WriteFile(filepath.Join(root, fmt.Sprint(pid), "cmdline"), cmd, 0600); e != nil {
			t.Fatal(e)
		}
	}
	found, e := v.List(context.Background(), install)
	if e != nil || len(found) != 2 {
		t.Fatalf("scoped process scan %+v %v", found, e)
	}
	other, e := v.List(context.Background(), "22222222-2222-4222-8222-222222222222")
	if e != nil || len(other) != 0 {
		t.Fatal("another installation's process included", e)
	}
	v.ReadFile = func(path string) ([]byte, error) {
		if strings.HasSuffix(path, "/71/cmdline") {
			return nil, fs.ErrPermission
		}
		return os.ReadFile(path)
	}
	if _, e = v.List(context.Background(), install); e == nil {
		t.Fatal("partial proc permission scan claimed no other issuer")
	}
}
