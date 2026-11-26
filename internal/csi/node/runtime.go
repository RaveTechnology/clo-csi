package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"time"
)

type Runtime struct {
	Inspector      Inspector
	Journal        *Journal
	Verifier       *Verifier
	Node           func() protocol.NodeRef
	Filesystem     Filesystem
	InstallationID string
}

func missing(err error) bool {
	var e *protocol.Error
	return errors.As(err, &e) && e.Code == protocol.NotFound
}
func (r *Runtime) Execute(ctx context.Context, job protocol.Job) (protocol.Receipt, error) {
	var receipt protocol.Receipt
	if job.ID == "" || job.Nonce == "" || job.Node != r.Node() {
		return receipt, protocol.Fail(protocol.Invalid, "job identity or node generation differs")
	}
	raw, e := json.Marshal(job)
	if e != nil {
		return receipt, e
	}
	h := sha256.Sum256(raw)
	fingerprint := hex.EncodeToString(h[:])
	key := "job:" + job.ID
	e = r.Journal.WithLock(ctx, key, func() error {
		state, e := r.Journal.Load(key)
		if e != nil && !missing(e) {
			return e
		}
		if e == nil && state.JobHash != fingerprint {
			return protocol.Fail(protocol.Blocked, "job ID reused with different input")
		}
		if state.Receipt != nil {
			receipt = *state.Receipt
			return nil
		}
		state = LocalState{Key: key, JobHash: fingerprint, Node: job.Node, Volume: job.Volume}
		if e = r.Journal.Save(state); e != nil {
			return e
		}
		receipt = protocol.Receipt{JobID: job.ID, Nonce: job.Nonce, Type: job.Type, Node: job.Node, Volume: job.Volume, ObservedAt: time.Now().UTC()}
		switch job.Type {
		case "baseline", "observe":
			receipt.Snapshot, e = r.Inspector.Snapshot(ctx)
		case "locateIssuer":
			if r.Verifier == nil {
				return protocol.Fail(protocol.Unavailable, "host verifier unavailable")
			}
			receipt.Issuer, e = r.Verifier.Locate(ctx, job.PodUID, job.ProcessNonce)
		case "verifyIssuer":
			if r.Verifier == nil {
				return protocol.Fail(protocol.Unavailable, "host verifier unavailable")
			}
			receipt.Proof, e = r.Verifier.Verify(ctx, job.Issuer)
		case "quiesce", "probeBlank", "quiesceBlank":
			e = r.Journal.WithLock(ctx, job.Volume.Handle, func() error {
				local, e := r.Journal.Load(job.Volume.Handle)
				if missing(e) {
					local = LocalState{Key: job.Volume.Handle, Volume: job.Volume, Node: job.Node}
				} else if e != nil {
					return e
				}
				if local.Volume != job.Volume || local.Node != job.Node {
					return protocol.Fail(protocol.Blocked, "quiesce generation differs")
				}
				if job.Type != "probeBlank" {
					local.Barrier = true
					local.DetachCommitted = true
					if e = r.Journal.Save(local); e != nil {
						return e
					}
				}
				if job.Type != "quiesce" && local.Initialization == "FormatComplete" {
					return protocol.Fail(protocol.Blocked, "completed local format cannot prove blank")
				}
				if local.StagePath != "" || len(local.Targets) > 0 || len(local.PendingTargets) > 0 {
					return protocol.Fail(protocol.Blocked, "local stage/pod references remain")
				}
				snap, e := r.Inspector.Snapshot(ctx)
				if e != nil {
					return e
				}
				d, e := SelectDevice(snap, job.Volume.Serial)
				if e != nil {
					return e
				}
				if len(d.Mounts) > 0 {
					return protocol.Fail(protocol.Blocked, "device still mounted")
				}
				if r.Filesystem == nil {
					return protocol.Fail(protocol.Unavailable, "kernel device verifier unavailable")
				}
				if e = r.Filesystem.VerifyUnmounted(ctx, d); e != nil {
					return e
				}
				if job.Type != "quiesce" {
					if d.FSType != "" || d.FSUUID != "" {
						return protocol.Fail(protocol.Blocked, "blank device inventory required")
					}
					signatures, err := r.Filesystem.Probe(ctx, d)
					if err != nil {
						return err
					}
					if len(signatures) != 0 {
						return protocol.Fail(protocol.Blocked, "raw device contains a recognized signature")
					}
					receipt.BlankChecked = true
				}
				receipt.Snapshot = snap
				receipt.Quiesced = job.Type != "probeBlank"
				return nil
			})
		case "listIssuers":
			if r.Verifier == nil || r.InstallationID == "" || job.InstallationID != r.InstallationID {
				return protocol.Fail(protocol.Blocked, "scoped host process verifier unavailable")
			}
			receipt.Issuers, e = r.Verifier.List(ctx, r.InstallationID)
		case "verifyFormat":
			if r.Filesystem == nil {
				return protocol.Fail(protocol.Unavailable, "filesystem verifier unavailable")
			}
			e = r.Journal.WithLock(ctx, job.Volume.Handle, func() error {
				snap, e := r.Inspector.Snapshot(ctx)
				if e != nil {
					return e
				}
				d, e := selected(snap, job.Volume.Serial)
				if e != nil {
					return e
				}
				if len(d.Mounts) > 0 {
					return protocol.Fail(protocol.RecoveryRequired, "format verification requires unmounted device")
				}
				sigs, e := r.Filesystem.Probe(ctx, d)
				if e != nil {
					return e
				}
				if job.Volume.FSUUID == "" || job.Volume.FSUUID != job.Volume.ProviderID || !ownSignature(sigs, job.Volume.FSUUID) {
					return protocol.Fail(protocol.RecoveryRequired, "own filesystem signature not proven")
				}
				d.FSType = "ext4"
				d.FSUUID = job.Volume.FSUUID
				if e = r.Filesystem.Check(ctx, d); e != nil {
					return protocol.Fail(protocol.RecoveryRequired, "read-only filesystem verification failed")
				}
				receipt.Snapshot = enhance(snap, d.Serial, job.Volume.FSUUID)
				receipt.FilesystemChecked = true
				return nil
			})
		default:
			return protocol.Fail(protocol.Invalid, "unsupported node job")
		}
		if e != nil {
			return e
		}
		state.Receipt = &receipt
		return r.Journal.Save(state)
	})
	return receipt, e
}
