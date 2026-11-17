package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"path/filepath"
	"time"
)

type LocalState struct {
	Schema            int                `json:"schema"`
	Key               string             `json:"key"`
	Generation        int64              `json:"generation"`
	Volume            protocol.VolumeRef `json:"volume"`
	Node              protocol.NodeRef   `json:"node"`
	Initialization    string             `json:"initialization"`
	Barrier           bool               `json:"barrier"`
	StagePath         string             `json:"stagePath"`
	Targets           map[string]bool    `json:"targets"`
	PendingTargets    map[string]bool    `json:"pendingTargets,omitempty"`
	DetachCommitted   bool               `json:"detachCommitted"`
	DetachReceipt     *protocol.Receipt  `json:"detachReceipt,omitempty"`
	FormatOperationID string             `json:"formatOperationID,omitempty"`
	FormatNonce       string             `json:"formatNonce,omitempty"`
	JobHash           string             `json:"jobHash,omitempty"`
	Receipt           *protocol.Receipt  `json:"receipt,omitempty"`
}
type Journal struct {
	root string
	Hook func(string) error
}

func NewJournal(root string) (*Journal, error) {
	if !filepath.IsAbs(root) {
		return nil, protocol.Fail(protocol.Invalid, "journal root must be absolute")
	}
	if e := os.MkdirAll(root, 0700); e != nil {
		return nil, e
	}
	st, e := os.Lstat(root)
	if e != nil {
		return nil, e
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return nil, protocol.Fail(protocol.Invalid, "journal root must be a real directory")
	}
	return &Journal{root: root}, nil
}
func (j *Journal) path(key, suffix string) string {
	h := sha256.Sum256([]byte(key))
	return filepath.Join(j.root, hex.EncodeToString(h[:])+suffix)
}
func (j *Journal) Load(key string) (LocalState, error) {
	f, e := os.OpenFile(j.path(key, ".json"), os.O_RDONLY|unix.O_NOFOLLOW, 0)
	if errors.Is(e, os.ErrNotExist) {
		return LocalState{}, protocol.Fail(protocol.NotFound, "local state missing")
	}
	if e != nil {
		return LocalState{}, e
	}
	defer f.Close()
	var s LocalState
	d := json.NewDecoder(io.LimitReader(f, 4<<20))
	if d.Decode(&s) != nil || s.Schema != 1 || s.Key != key {
		return s, protocol.Fail(protocol.RecoveryRequired, "invalid local journal")
	}
	return s, nil
}
func (j *Journal) Save(s LocalState) error {
	if s.Key == "" {
		return protocol.Fail(protocol.Invalid, "journal key empty")
	}
	s.Schema = 1
	raw, e := json.Marshal(s)
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(j.root, ".pending-")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	defer f.Close()
	if e = f.Chmod(0600); e != nil {
		return e
	}
	if _, e = f.Write(raw); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if j.Hook != nil {
		if e = j.Hook("file-synced"); e != nil {
			return e
		}
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Rename(name, j.path(s.Key, ".json")); e != nil {
		return e
	}
	if j.Hook != nil {
		if e = j.Hook("renamed"); e != nil {
			return e
		}
	}
	dir, e := os.Open(j.root)
	if e != nil {
		return e
	}
	defer dir.Close()
	if e = dir.Sync(); e != nil {
		return e
	}
	if j.Hook != nil {
		return j.Hook("directory-synced")
	}
	return nil
}
func (j *Journal) WithLock(ctx context.Context, key string, fn func() error) error {
	f, e := os.OpenFile(j.path(key, ".lock"), os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	for {
		e = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if e == nil {
			break
		}
		if e != unix.EWOULDBLOCK && e != unix.EAGAIN {
			return e
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	defer unix.Flock(int(f.Fd()), unix.LOCK_UN)
	return fn()
}
