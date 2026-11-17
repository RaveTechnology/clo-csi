package node

import (
	"context"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"os/exec"
	"time"
)

// HostRunner enters only PID1's mount namespace/root. Jobs cannot choose a PID,
// namespace, executable or arbitrary command; callers construct fixed arguments.
type HostRunner struct{}

func (HostRunner) Run(ctx context.Context, binary string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	argv := []string{"--mount=/proc/1/ns/mnt", "--root=/proc/1/root", "--wd=/", "--", binary}
	argv = append(argv, args...)
	command := exec.CommandContext(ctx, "/usr/bin/nsenter", argv...)
	command.Env = []string{"LC_ALL=C", "LANG=C", "PATH=/usr/sbin:/usr/bin:/sbin:/bin"}
	out, e := command.Output()
	if e != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, protocol.Fail(protocol.Unavailable, "host command failed: %s", binary)
	}
	return out, nil
}
