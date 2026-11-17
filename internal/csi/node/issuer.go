package node

import (
	"context"
	"errors"
	"fmt"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/google/uuid"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

type Verifier struct {
	ProcRoot string
	Node     func() protocol.NodeRef
	ReadFile func(string) ([]byte, error)
}

func (v *Verifier) read(path string) ([]byte, error) {
	if v.ReadFile != nil {
		return v.ReadFile(filepath.Join(v.ProcRoot, path))
	}
	return os.ReadFile(filepath.Join(v.ProcRoot, path))
}
func (v *Verifier) boot() (string, error) {
	b, e := v.read("sys/kernel/random/boot_id")
	if e != nil {
		return "", e
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return "", fmt.Errorf("empty host boot ID")
	}
	return s, nil
}
func startTicks(stat []byte) (uint64, error) {
	end := strings.LastIndex(string(stat), ") ")
	if end < 0 {
		return 0, fmt.Errorf("invalid proc stat")
	}
	fields := strings.Fields(string(stat[end+2:]))
	if len(fields) < 20 {
		return 0, fmt.Errorf("truncated proc stat")
	}
	return strconv.ParseUint(fields[19], 10, 64)
}
func (v *Verifier) Locate(ctx context.Context, podUID, nonce string) (protocol.IssuerDescriptor, error) {
	if _, e := uuid.Parse(podUID); e != nil || !regexp.MustCompile(`^[a-zA-Z0-9-]{1,128}$`).MatchString(nonce) {
		return protocol.IssuerDescriptor{}, protocol.Fail(protocol.Invalid, "invalid issuer lookup")
	}
	n := v.Node()
	if n.UID == "" || n.BindingID == "" || n.ServerID == "" {
		return protocol.IssuerDescriptor{}, protocol.Fail(protocol.Blocked, "host binding not established")
	}
	boot, e := v.boot()
	if e != nil {
		return protocol.IssuerDescriptor{}, protocol.Fail(protocol.Unavailable, "host procfs unavailable")
	}
	entries, e := os.ReadDir(v.ProcRoot)
	if e != nil {
		return protocol.IssuerDescriptor{}, e
	}
	podPattern := regexp.MustCompile(`(?:^|[-/])pod` + regexp.QuoteMeta(strings.ReplaceAll(podUID, "-", "_")) + `(?:[./]|$)`)
	podPattern2 := regexp.MustCompile(`(?:^|[-/])pod` + regexp.QuoteMeta(podUID) + `(?:[./]|$)`)
	var found *protocol.IssuerDescriptor
	for _, entry := range entries {
		if e = ctx.Err(); e != nil {
			return protocol.IssuerDescriptor{}, e
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		cmd, err := v.read(entry.Name() + "/cmdline")
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return protocol.IssuerDescriptor{}, protocol.Fail(protocol.Unavailable, "cannot inspect host process")
		}
		match := false
		for _, arg := range strings.Split(string(cmd), "\x00") {
			match = match || arg == "--process-nonce="+nonce
		}
		if !match {
			continue
		}
		cgroup, err := v.read(entry.Name() + "/cgroup")
		if err != nil {
			return protocol.IssuerDescriptor{}, protocol.Fail(protocol.Unavailable, "cannot inspect issuer cgroup")
		}
		if !podPattern.Match(cgroup) && !podPattern2.Match(cgroup) {
			continue
		}
		stat, err := v.read(entry.Name() + "/stat")
		if err != nil {
			return protocol.IssuerDescriptor{}, protocol.Fail(protocol.Unavailable, "cannot inspect issuer start time")
		}
		ticks, err := startTicks(stat)
		if err != nil || ticks == 0 {
			return protocol.IssuerDescriptor{}, protocol.Fail(protocol.RecoveryRequired, "invalid issuer start time")
		}
		if found != nil {
			return protocol.IssuerDescriptor{}, protocol.Fail(protocol.RecoveryRequired, "multiple issuer processes")
		}
		d := protocol.IssuerDescriptor{PodUID: podUID, ProcessNonce: nonce, NodeUID: n.UID, BindingID: n.BindingID, ServerID: n.ServerID, HostBootID: boot, HostPID: pid, StartTicks: ticks}
		found = &d
	}
	if found == nil {
		return protocol.IssuerDescriptor{}, protocol.Fail(protocol.NotFound, "issuer process not found on bound host")
	}
	return *found, nil
}
func (v *Verifier) Verify(ctx context.Context, d protocol.IssuerDescriptor) (protocol.IssuerProof, error) {
	p := protocol.IssuerProof{Descriptor: d, Verdict: protocol.Unknown}
	if e := ctx.Err(); e != nil {
		return p, e
	}
	n := v.Node()
	if !d.Valid() || n.UID != d.NodeUID || n.ServerID != d.ServerID || n.BindingID != d.BindingID {
		p.Reason = "host binding differs"
		return p, nil
	}
	boot, e := v.boot()
	if e != nil {
		p.Reason = "host procfs unavailable"
		return p, nil
	}
	p.BootID = boot
	if boot != d.HostBootID {
		p.Verdict = protocol.Gone
		p.Reason = "same VM rebooted"
		return p, nil
	}
	stat, e := v.read(fmt.Sprintf("%d/stat", d.HostPID))
	if errors.Is(e, fs.ErrNotExist) {
		p.Verdict = protocol.Gone
		p.Reason = "PID absent"
		return p, nil
	}
	if e != nil {
		p.Reason = "cannot read process"
		return p, nil
	}
	ticks, e := startTicks(stat)
	if e != nil {
		p.Reason = "invalid process stat"
		return p, nil
	}
	if ticks != d.StartTicks {
		p.Verdict = protocol.Gone
		p.Reason = "PID incarnation changed"
	} else {
		p.Verdict = protocol.Alive
		p.Reason = "process still exists"
	}
	return p, nil
}

func argument(args []string, name string) string {
	value := ""
	for i, arg := range args {
		if strings.HasPrefix(arg, name+"=") {
			value = strings.TrimPrefix(arg, name+"=")
		} else if arg == name && i+1 < len(args) {
			value = args[i+1]
		}
	}
	return value
}

// List is a complete read-only host scan for this project/installation. A
// permission error never becomes a claim that another controller is absent.
func (v *Verifier) List(ctx context.Context, installationID string) ([]protocol.IssuerDescriptor, error) {
	if _, e := uuid.Parse(installationID); e != nil {
		return nil, protocol.Fail(protocol.Invalid, "installation UUID required for process scan")
	}
	n := v.Node()
	if n.ProjectID == "" || n.BindingID == "" {
		return nil, protocol.Fail(protocol.Blocked, "verified host binding required")
	}
	entries, e := os.ReadDir(v.ProcRoot)
	if e != nil {
		return nil, protocol.Fail(protocol.Unavailable, "host procfs unavailable")
	}
	podPattern := regexp.MustCompile(`(?:^|[-/])pod([0-9a-fA-F]{8}[-_][0-9a-fA-F]{4}[-_][0-9a-fA-F]{4}[-_][0-9a-fA-F]{4}[-_][0-9a-fA-F]{12})(?:[./]|$)`)
	var out []protocol.IssuerDescriptor
	for _, entry := range entries {
		if e = ctx.Err(); e != nil {
			return nil, e
		}
		pid, e := strconv.Atoi(entry.Name())
		if e != nil || pid <= 0 {
			continue
		}
		raw, e := v.read(entry.Name() + "/cmdline")
		if errors.Is(e, fs.ErrNotExist) {
			continue
		}
		if e != nil {
			return nil, protocol.Fail(protocol.Unavailable, "complete host process scan unavailable")
		}
		args := strings.Split(string(raw), "\x00")
		if len(args) < 2 || args[1] != "controller" || argument(args, "--project-id") != n.ProjectID || argument(args, "--installation-id") != installationID {
			continue
		}
		nonce := argument(args, "--process-nonce")
		if nonce == "" {
			return nil, protocol.Fail(protocol.Blocked, "scoped controller process has unknown incarnation")
		}
		group, e := v.read(entry.Name() + "/cgroup")
		if e != nil {
			return nil, protocol.Fail(protocol.Unavailable, "controller cgroup unavailable")
		}
		matches := podPattern.FindAllSubmatch(group, -1)
		if len(matches) != 1 {
			return nil, protocol.Fail(protocol.Blocked, "scoped controller has unknown pod cgroup")
		}
		podUID := strings.ReplaceAll(string(matches[0][1]), "_", "-")
		descriptor, e := v.Locate(ctx, podUID, nonce)
		if codeNotFound(e) {
			continue
		}
		if e != nil {
			return nil, e
		}
		if descriptor.HostPID != pid {
			return nil, protocol.Fail(protocol.Blocked, "scoped controller process identity changed during scan")
		}
		out = append(out, descriptor)
	}
	return out, nil
}
func codeNotFound(err error) bool {
	var e *protocol.Error
	return errors.As(err, &e) && e.Code == protocol.NotFound
}
