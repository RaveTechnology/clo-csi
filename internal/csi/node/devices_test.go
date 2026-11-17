package node

import (
	"context"
	"encoding/json"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"os"
	"testing"
)

func captured(t *testing.T, name string) ([]byte, []byte, string) {
	t.Helper()
	b, e := os.ReadFile("testdata/" + name + ".json")
	if e != nil {
		t.Fatal(e)
	}
	var x struct {
		Blockdevices json.RawMessage `json:"blockdevices"`
		Filesystems  json.RawMessage `json:"filesystems"`
		BootID       string          `json:"bootID"`
	}
	if e = json.Unmarshal(b, &x); e != nil {
		t.Fatal(e)
	}
	ls, _ := json.Marshal(map[string]json.RawMessage{"blockdevices": x.Blockdevices})
	fm, _ := json.Marshal(map[string]json.RawMessage{"filesystems": x.Filesystems})
	return ls, fm, x.BootID
}

type fixedInspector struct {
	snapshot protocol.Snapshot
	err      error
	calls    int
}

func (i *fixedInspector) Snapshot(context.Context) (protocol.Snapshot, error) {
	i.calls++
	return i.snapshot, i.err
}
func (i *fixedInspector) BySerial(ctx context.Context, serial string) (Device, error) {
	return SelectDevice(i.snapshot, serial)
}
func TestSerialSurvivesApiPathSwap(t *testing.T) {
	for _, cycle := range []string{"0", "1"} {
		var paths []string
		for _, phase := range []string{"hot", "reboot"} {
			ls, fm, boot := captured(t, "order-"+cycle+"-"+phase)
			s, e := ParseSnapshot(ls, fm, boot)
			if e != nil {
				t.Fatal(e)
			}
			d, e := SelectDevice(s, "6519968c-695c-4511-bd21-b77833a06470")
			if e != nil {
				t.Fatal(e)
			}
			paths = append(paths, d.Path)
			for _, disk := range s.Disks {
				if disk.Serial == "a5846adf-5e0c-4fe4-a659-4b16b198a4e6" && !disk.Root {
					t.Fatal("partition root ancestry lost")
				}
			}
		}
		if paths[0] == paths[1] {
			t.Fatalf("fixture did not swap: %v", paths)
		}
	}
}
func TestDuplicateSerialAndRootRejected(t *testing.T) {
	ls, fm, boot := captured(t, "order-0-hot")
	s, e := ParseSnapshot(ls, fm, boot)
	if e != nil {
		t.Fatal(e)
	}
	rootSerial := ""
	for _, d := range s.Disks {
		if d.Root {
			rootSerial = d.Serial
		}
	}
	if rootSerial == "" {
		t.Fatal("fixture root missing")
	}
	if _, e = SelectDevice(s, rootSerial); e == nil {
		t.Fatal("root selected")
	}
	s.Disks = append(s.Disks, s.Disks[1])
	if _, e = SelectDevice(s, s.Disks[1].Serial); e == nil {
		t.Fatal("duplicate selected")
	}
	if _, e = SelectDevice(s, ""); e == nil {
		t.Fatal("empty serial accepted")
	}
	var x map[string]any
	_ = json.Unmarshal(ls, &x)
	disks := x["blockdevices"].([]any)
	disks[1].(map[string]any)["serial"] = nil
	bad, _ := json.Marshal(x)
	if _, e = ParseSnapshot(bad, fm, boot); e == nil {
		t.Fatal("missing serial ignored")
	}
}
