package node

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"path/filepath"
	"sort"
	"strings"
)

type Device struct{ protocol.Disk }
type Inspector interface {
	Snapshot(context.Context) (protocol.Snapshot, error)
	BySerial(context.Context, string) (Device, error)
}
type Runner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}
type LinuxInspector struct{ Runner Runner }

func (i *LinuxInspector) Snapshot(ctx context.Context) (protocol.Snapshot, error) {
	ls, e := i.Runner.Run(ctx, "/usr/bin/lsblk", "--json", "--bytes", "--paths", "--tree", "--output", "NAME,MAJ:MIN,TYPE,SERIAL,FSTYPE,UUID,SIZE,MOUNTPOINTS")
	if e != nil {
		return protocol.Snapshot{}, e
	}
	fm, e := i.Runner.Run(ctx, "/usr/bin/findmnt", "--json", "--list", "--output", "SOURCE,TARGET,MAJ:MIN,OPTIONS")
	if e != nil {
		return protocol.Snapshot{}, e
	}
	boot, e := i.Runner.Run(ctx, "/usr/bin/cat", "/proc/sys/kernel/random/boot_id")
	if e != nil {
		return protocol.Snapshot{}, e
	}
	return ParseSnapshot(ls, fm, strings.TrimSpace(string(boot)))
}
func (i *LinuxInspector) BySerial(ctx context.Context, s string) (Device, error) {
	snap, e := i.Snapshot(ctx)
	if e != nil {
		return Device{}, e
	}
	return SelectDevice(snap, s)
}

type block struct {
	Name        string    `json:"name"`
	MajorMinor  string    `json:"maj:min"`
	Type        string    `json:"type"`
	Serial      string    `json:"serial"`
	FSType      string    `json:"fstype"`
	UUID        string    `json:"uuid"`
	Size        int64     `json:"size"`
	Mountpoints []*string `json:"mountpoints"`
	Children    []block   `json:"children"`
}
type mountEntry struct {
	Source     string       `json:"source"`
	Target     string       `json:"target"`
	MajorMinor string       `json:"maj:min"`
	Options    string       `json:"options"`
	Children   []mountEntry `json:"children"`
}

func ParseSnapshot(ls, fm []byte, bootID string) (protocol.Snapshot, error) {
	var blocks struct {
		Devices []block `json:"blockdevices"`
	}
	var mounts struct {
		Filesystems []mountEntry `json:"filesystems"`
	}
	if json.Unmarshal(ls, &blocks) != nil || json.Unmarshal(fm, &mounts) != nil || bootID == "" {
		return protocol.Snapshot{}, protocol.Fail(protocol.RecoveryRequired, "invalid host inventory")
	}
	flat := []mountEntry{}
	var flatten func([]mountEntry)
	flatten = func(ms []mountEntry) {
		for _, m := range ms {
			flat = append(flat, m)
			flatten(m.Children)
		}
	}
	flatten(mounts.Filesystems)
	s := protocol.Snapshot{BootID: bootID}
	seen := map[string]bool{}
	rootCount := 0
	for _, b := range blocks.Devices {
		if b.Type != "disk" {
			continue
		}
		serial := strings.TrimSpace(b.Serial)
		if serial == "" || seen[serial] || !strings.HasPrefix(b.Name, "/dev/") || filepath.Clean(b.Name) != b.Name || b.MajorMinor == "" {
			return s, protocol.Fail(protocol.RecoveryRequired, "missing/duplicate disk identity")
		}
		seen[serial] = true
		disk := protocol.Disk{Path: b.Name, Serial: serial, MajorMinor: b.MajorMinor, FSType: b.FSType, FSUUID: b.UUID, SizeBytes: b.Size, Partitioned: len(b.Children) > 0}
		majors := map[string]bool{}
		var ancestry func(block)
		ancestry = func(x block) {
			majors[x.MajorMinor] = true
			for _, m := range x.Mountpoints {
				if m != nil && *m == "/" {
					disk.Root = true
				}
			}
			for _, c := range x.Children {
				ancestry(c)
			}
		}
		ancestry(b)
		for _, m := range flat {
			if !majors[m.MajorMinor] {
				continue
			}
			if m.Target == "/" {
				disk.Root = true
			}
			ro := false
			for _, o := range strings.Split(m.Options, ",") {
				ro = ro || o == "ro"
			}
			disk.Mounts = append(disk.Mounts, protocol.Mount{Target: m.Target, Source: m.Source, MajorMinor: m.MajorMinor, ReadOnly: ro})
		}
		if disk.Root {
			rootCount++
		}
		s.Disks = append(s.Disks, disk)
	}
	if rootCount != 1 {
		return s, protocol.Fail(protocol.RecoveryRequired, "root disk ancestry must be unique")
	}
	sort.Slice(s.Disks, func(a, b int) bool { return s.Disks[a].Serial < s.Disks[b].Serial })
	return s, nil
}
func SelectDevice(s protocol.Snapshot, serial string) (Device, error) {
	if serial == "" {
		return Device{}, protocol.Fail(protocol.Invalid, "empty serial")
	}
	var found *protocol.Disk
	for _, d := range s.Disks {
		if d.Serial != serial {
			continue
		}
		if found != nil {
			return Device{}, protocol.Fail(protocol.RecoveryRequired, "serial is not unique")
		}
		copy := d
		found = &copy
	}
	if found == nil {
		return Device{}, protocol.Fail(protocol.NotFound, "serial not present")
	}
	if found.Root || found.Partitioned {
		return Device{}, protocol.Fail(protocol.Blocked, "root or partitioned disk is ineligible")
	}
	if !strings.HasPrefix(found.Path, "/dev/") || filepath.Clean(found.Path) != found.Path || found.MajorMinor == "" {
		return Device{}, fmt.Errorf("invalid canonical device identity")
	}
	return Device{*found}, nil
}
