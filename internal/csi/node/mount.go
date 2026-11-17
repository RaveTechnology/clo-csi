package node

import (
	"context"
	"encoding/json"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/google/uuid"
	"golang.org/x/sys/unix"
	mount "k8s.io/mount-utils"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type HostFilesystem struct {
	Runner      Runner
	Inspector   Inspector
	HostRoot    string
	guard       func(Device, bool) error
	writeRescan func(string, []byte) error
}

func blockGuard(d Device, exclusive bool) error {
	if filepath.Dir(d.Path) != "/dev" || filepath.Clean(d.Path) != d.Path {
		return protocol.Fail(protocol.Blocked, "noncanonical device path")
	}
	parts := strings.Split(d.MajorMinor, ":")
	if len(parts) != 2 {
		return protocol.Fail(protocol.Blocked, "invalid block identity")
	}
	major, e := strconv.ParseUint(parts[0], 10, 32)
	if e != nil {
		return protocol.Fail(protocol.Blocked, "invalid block major")
	}
	minor, e := strconv.ParseUint(parts[1], 10, 32)
	if e != nil {
		return protocol.Fail(protocol.Blocked, "invalid block minor")
	}
	flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC | unix.O_NONBLOCK
	if exclusive {
		flags |= unix.O_EXCL
	}
	fd, e := unix.Open(d.Path, flags, 0)
	if e != nil {
		return protocol.Fail(protocol.Blocked, "block device unavailable, mounted, or symlinked")
	}
	defer unix.Close(fd)
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || stat.Mode&unix.S_IFMT != unix.S_IFBLK || uint64(unix.Major(uint64(stat.Rdev))) != major || uint64(unix.Minor(uint64(stat.Rdev))) != minor {
		return protocol.Fail(protocol.Blocked, "actual block identity differs")
	}
	return nil
}
func (h *HostFilesystem) confirm(ctx context.Context, d Device, unmounted bool) error {
	if h.Inspector == nil || h.Runner == nil {
		return protocol.Fail(protocol.Unavailable, "host filesystem configuration incomplete")
	}
	s, e := h.Inspector.Snapshot(ctx)
	if e != nil {
		return e
	}
	actual, e := selected(s, d.Serial)
	if e != nil {
		return e
	}
	if actual.Path != d.Path || actual.MajorMinor != d.MajorMinor {
		return protocol.Fail(protocol.Blocked, "device path/physical identity changed")
	}
	if unmounted && len(actual.Mounts) > 0 {
		return protocol.Fail(protocol.Blocked, "device still has host mounts")
	}
	if h.guard != nil {
		return h.guard(d, unmounted)
	}
	return blockGuard(d, unmounted)
}
func (h *HostFilesystem) Probe(ctx context.Context, d Device) ([]Signature, error) {
	if e := h.confirm(ctx, d, false); e != nil {
		return nil, e
	}
	raw, e := h.Runner.Run(ctx, "/usr/sbin/wipefs", "--json", "--no-act", d.Path)
	if e != nil {
		return nil, e
	}
	var result struct {
		Signatures *[]struct {
			Type string `json:"type"`
			UUID string `json:"uuid"`
		} `json:"signatures"`
	}
	if json.Unmarshal(raw, &result) != nil || result.Signatures == nil {
		return nil, protocol.Fail(protocol.RecoveryRequired, "invalid filesystem signature inventory")
	}
	var out []Signature
	for _, s := range *result.Signatures {
		if s.Type == "" {
			return nil, protocol.Fail(protocol.RecoveryRequired, "incomplete filesystem signature")
		}
		out = append(out, Signature{Type: s.Type, UUID: s.UUID})
	}
	return out, nil
}
func (h *HostFilesystem) Format(ctx context.Context, d Device, fsUUID string) error {
	if _, e := uuid.Parse(fsUUID); e != nil {
		return protocol.Fail(protocol.Invalid, "filesystem UUID required")
	}
	if e := h.confirm(ctx, d, true); e != nil {
		return e
	}
	signatures, e := h.Probe(ctx, d)
	if e != nil {
		return e
	}
	if len(signatures) > 0 {
		return protocol.Fail(protocol.Blocked, "existing signature prevents mkfs")
	}
	_, e = h.Runner.Run(ctx, "/usr/sbin/mkfs.ext4", "-q", "-U", fsUUID, "-E", "lazy_itable_init=0,lazy_journal_init=0", d.Path)
	return e
}
func (h *HostFilesystem) Check(ctx context.Context, d Device) error {
	if e := h.confirm(ctx, d, true); e != nil {
		return e
	}
	sigs, e := h.Probe(ctx, d)
	if e != nil {
		return e
	}
	if d.FSUUID == "" || !ownSignature(sigs, d.FSUUID) {
		return protocol.Fail(protocol.RecoveryRequired, "read-only check requires own ext4 filesystem")
	}
	_, e = h.Runner.Run(ctx, "/usr/sbin/e2fsck", "-f", "-n", d.Path)
	return e
}
func (h *HostFilesystem) Mount(ctx context.Context, d Device, target string) error {
	if e := h.confirm(ctx, d, true); e != nil {
		return e
	}
	sigs, e := h.Probe(ctx, d)
	if e != nil {
		return e
	}
	if d.FSUUID == "" || !ownSignature(sigs, d.FSUUID) {
		return protocol.Fail(protocol.Blocked, "mount filesystem identity differs")
	}
	args := mount.MakeMountArgs(d.Path, target, "ext4", []string{"rw", "nosuid", "nodev"})
	_, e = h.Runner.Run(ctx, "/usr/bin/mount", args...)
	return e
}
func (h *HostFilesystem) Bind(ctx context.Context, source, target string, readonly bool) error {
	args := mount.MakeMountArgs(source, target, "", []string{"bind"})
	if _, e := h.Runner.Run(ctx, "/usr/bin/mount", args...); e != nil {
		return e
	}
	if readonly {
		return h.Remount(ctx, source, target, true)
	}
	return nil
}
func (h *HostFilesystem) Remount(ctx context.Context, source, target string, readonly bool) error {
	mode := "rw"
	if readonly {
		mode = "ro"
	}
	args := mount.MakeMountArgs(source, target, "", []string{"bind", "remount", mode, "nosuid", "nodev"})
	_, e := h.Runner.Run(ctx, "/usr/bin/mount", args...)
	return e
}
func (h *HostFilesystem) Unmount(ctx context.Context, target string) error {
	_, e := h.Runner.Run(ctx, "/usr/bin/umount", target)
	return e
}

// O_EXCL on a block device rejects a mounted filesystem even when its mount
// lives in another namespace and is absent from PID1's findmnt inventory.
func (h *HostFilesystem) VerifyUnmounted(ctx context.Context, d Device) error {
	return h.confirm(ctx, d, true)
}
func unsigned(value string) (uint64, error) {
	v, e := strconv.ParseUint(strings.TrimSpace(value), 10, 63)
	if e != nil {
		return 0, protocol.Fail(protocol.RecoveryRequired, "invalid host filesystem numeric result")
	}
	return v, nil
}
func (h *HostFilesystem) Stats(ctx context.Context, target string) (Usage, error) {
	raw, e := h.Runner.Run(ctx, "/usr/bin/stat", "-f", "-c", "%S %b %a %f %c %d", target)
	if e != nil {
		return Usage{}, e
	}
	fields := strings.Fields(string(raw))
	if len(fields) != 6 {
		return Usage{}, protocol.Fail(protocol.RecoveryRequired, "invalid actual statfs result")
	}
	values := make([]uint64, 6)
	for i, s := range fields {
		values[i], e = unsigned(s)
		if e != nil {
			return Usage{}, e
		}
	}
	bs, total, available, free, inodes, freeInodes := values[0], values[1], values[2], values[3], values[4], values[5]
	if bs == 0 || total > math.MaxInt64/bs || free > total || available > total || freeInodes > inodes {
		return Usage{}, protocol.Fail(protocol.RecoveryRequired, "statfs arithmetic overflow or inconsistent counts")
	}
	return Usage{TotalBytes: int64(bs * total), UsedBytes: int64(bs * (total - free)), AvailableBytes: int64(bs * available), TotalInodes: int64(inodes), UsedInodes: int64(inodes - freeInodes), AvailableInodes: int64(freeInodes)}, nil
}
func (h *HostFilesystem) filesystemSize(ctx context.Context, d Device) (int64, error) {
	raw, e := h.Runner.Run(ctx, "/usr/sbin/dumpe2fs", "-h", d.Path)
	if e != nil {
		return 0, e
	}
	var count, size uint64
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "Block count:") {
			count, e = unsigned(strings.TrimPrefix(line, "Block count:"))
		}
		if strings.HasPrefix(line, "Block size:") {
			size, e = unsigned(strings.TrimPrefix(line, "Block size:"))
		}
		if e != nil {
			return 0, e
		}
	}
	if size == 0 || count == 0 || count > math.MaxInt64/size {
		return 0, protocol.Fail(protocol.RecoveryRequired, "invalid ext4 block count/size")
	}
	return int64(count * size), nil
}
func (h *HostFilesystem) Expand(ctx context.Context, d Device, required, limit int64) (int64, error) {
	if !validExpansionRange(required, limit) {
		return 0, protocol.Fail(protocol.Invalid, "invalid expansion range")
	}
	if e := h.confirm(ctx, d, false); e != nil {
		return 0, e
	}
	sigs, e := h.Probe(ctx, d)
	if e != nil {
		return 0, e
	}
	if d.FSUUID == "" || !ownSignature(sigs, d.FSUUID) {
		return 0, protocol.Fail(protocol.Blocked, "resize filesystem identity differs")
	}
	name := filepath.Base(d.Path)
	if !regexp.MustCompile(`^sd[a-z]+$`).MatchString(name) {
		return 0, protocol.Fail(protocol.Blocked, "only verified CLO SCSI device rescan supported")
	}
	root := h.HostRoot
	if root == "" {
		root = "/proc/1/root"
	}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return 0, protocol.Fail(protocol.Invalid, "invalid trusted host root")
	}
	path := filepath.Join(root, "sys/class/block", name, "device/rescan")
	write := h.writeRescan
	if write == nil {
		write = func(path string, data []byte) error {
			file, e := os.OpenFile(path, os.O_WRONLY|unix.O_NOFOLLOW, 0)
			if e != nil {
				return e
			}
			defer file.Close()
			n, e := file.Write(data)
			if e == nil && n != len(data) {
				return protocol.Fail(protocol.Unavailable, "partial SCSI rescan write")
			}
			return e
		}
	}
	if e = write(path, []byte("1\n")); e != nil {
		return 0, protocol.Fail(protocol.Unavailable, "SCSI rescan failed")
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var bytes int64
	for {
		raw, e := h.Runner.Run(waitCtx, "/usr/sbin/blockdev", "--getsize64", d.Path)
		if e != nil {
			return 0, e
		}
		value, e := unsigned(string(raw))
		if e != nil {
			return 0, e
		}
		if value == 0 || value > math.MaxInt64 {
			return 0, protocol.Fail(protocol.RecoveryRequired, "invalid actual block capacity")
		}
		bytes = int64(value)
		if bytes >= required {
			break
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return 0, protocol.Fail(protocol.Unavailable, "guest block capacity below expansion request")
		case <-timer.C:
		}
	}
	if limit > 0 && bytes > limit {
		return 0, protocol.Fail(protocol.Invalid, "actual block capacity exceeds expansion limit")
	}
	// Omitted capacity_range permits growth to the actual maximum. Do not
	// report the larger block size without first growing/verifying ext4.
	if required == 0 {
		required = bytes
	}
	before, e := h.filesystemSize(ctx, d)
	if e != nil {
		return 0, e
	}
	if before > bytes {
		return 0, protocol.Fail(protocol.RecoveryRequired, "ext4 exceeds actual block capacity; refusing resize")
	}
	if before >= required && before <= bytes {
		return bytes, nil
	}
	if _, e = h.Runner.Run(ctx, "/usr/sbin/resize2fs", d.Path); e != nil {
		return 0, e
	}
	if e = h.confirm(ctx, d, false); e != nil {
		return 0, e
	}
	after, e := h.filesystemSize(ctx, d)
	if e != nil {
		return 0, e
	}
	if after < required || after > bytes || after < before {
		return 0, protocol.Fail(protocol.RecoveryRequired, "actual ext4 growth not established")
	}
	return bytes, nil
}

var _ Filesystem = (*HostFilesystem)(nil)
