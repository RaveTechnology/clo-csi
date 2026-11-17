package node

import (
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"strings"
)

type Paths struct{ Root string }

// RemoveEmpty removes one already-verified owned directory, never its contents.
// Walk ancestors by fd with O_NOFOLLOW so a replaced symlink cannot redirect it.
func (p Paths) RemoveEmpty(path, kind string) error {
	if e := p.Check(path, kind, false); e != nil {
		return e
	}
	rel, _ := filepath.Rel(p.Root, path)
	parts := strings.Split(rel, string(os.PathSeparator))
	fd, e := unix.Open(p.Root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return protocol.Fail(protocol.Unavailable, "kubelet root unavailable for directory cleanup")
	}
	defer func() { unix.Close(fd) }()
	for _, part := range parts[:len(parts)-1] {
		next, e := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if e == unix.ENOENT {
			return nil
		}
		if e != nil {
			return protocol.Fail(protocol.Blocked, "cleanup path ancestor changed")
		}
		unix.Close(fd)
		fd = next
	}
	e = unix.Unlinkat(fd, parts[len(parts)-1], unix.AT_REMOVEDIR)
	if e == nil || e == unix.ENOENT {
		return nil
	}
	return protocol.Fail(protocol.Blocked, "owned mount directory is nonempty, busy or changed; no contents removed")
}

func (p Paths) Check(path, kind string, create bool) error {
	if !filepath.IsAbs(p.Root) || filepath.Clean(p.Root) != p.Root || !filepath.IsAbs(path) || filepath.Clean(path) != path || !strings.HasPrefix(path, p.Root+string(os.PathSeparator)) {
		return protocol.Fail(protocol.Invalid, "path must be canonical beneath kubelet root")
	}
	rel, e := filepath.Rel(p.Root, path)
	if e != nil {
		return protocol.Fail(protocol.Invalid, "invalid kubelet path")
	}
	stage := strings.HasPrefix(rel, filepath.Join("plugins/kubernetes.io/csi", protocol.DriverName)+"/")
	parts := strings.Split(rel, string(os.PathSeparator))
	target := len(parts) >= 6 && parts[0] == "pods" && parts[2] == "volumes" && parts[3] == "kubernetes.io~csi" && parts[len(parts)-1] == "mount"
	if kind == "stage" && !stage || kind == "target" && !target || kind == "any" && !stage && !target {
		return protocol.Fail(protocol.Invalid, "path outside owned CSI staging/pod mount layout")
	}
	fd, e := unix.Open(p.Root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return protocol.Fail(protocol.Invalid, "kubelet root unavailable or symlinked")
	}
	defer func() { unix.Close(fd) }()
	for _, part := range parts {
		next, e := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if e == unix.ENOENT && create {
			if e = unix.Mkdirat(fd, part, 0750); e != nil && e != unix.EEXIST {
				return protocol.Fail(protocol.Invalid, "cannot create owned mount directory")
			}
			next, e = unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		}
		if e == unix.ENOENT && !create {
			return nil
		}
		if e != nil {
			return protocol.Fail(protocol.Invalid, "mount path contains a symlink or non-directory")
		}
		unix.Close(fd)
		fd = next
	}
	return nil
}
