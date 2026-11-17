package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"sort"
)

type backupData struct {
	Schema       int          `json:"schema"`
	Installation Installation `json:"installation"`
	Documents    []Document   `json:"documents"`
}
type backupEnvelope struct {
	Data   json.RawMessage `json:"data"`
	SHA256 string          `json:"sha256"`
}

func (r *Repository) Export(ctx context.Context) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f, e := r.Frozen(ctx)
	if e != nil {
		return nil, e
	}
	if !f {
		return nil, protocol.Fail(protocol.Blocked, "freeze writers before export")
	}
	data := backupData{Schema: 1, Installation: r.installation}
	for _, kind := range Kinds {
		ds, e := r.backend.List(ctx, kind)
		if e != nil {
			return nil, e
		}
		data.Documents = append(data.Documents, ds...)
	}
	sort.Slice(data.Documents, func(i, j int) bool {
		a, b := data.Documents[i], data.Documents[j]
		return a.Kind+"/"+a.Name < b.Kind+"/"+b.Name
	})
	raw, e := json.Marshal(data)
	if e != nil {
		return nil, e
	}
	h := sha256.Sum256(raw)
	return json.Marshal(backupEnvelope{raw, hex.EncodeToString(h[:])})
}
func (r *Repository) Restore(ctx context.Context, blob []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	f, e := r.Frozen(ctx)
	if e != nil {
		return e
	}
	if !f {
		return protocol.Fail(protocol.Blocked, "freeze writers before restore")
	}
	var env backupEnvelope
	if json.Unmarshal(blob, &env) != nil {
		return protocol.Fail(protocol.Invalid, "invalid backup")
	}
	h := sha256.Sum256(env.Data)
	if hex.EncodeToString(h[:]) != env.SHA256 {
		return protocol.Fail(protocol.Invalid, "backup checksum differs")
	}
	var data backupData
	if json.Unmarshal(env.Data, &data) != nil || data.Schema != 1 || data.Installation != r.installation {
		return protocol.Fail(protocol.Invalid, "backup scope/schema differs")
	}
	// Validate the entire graph in an isolated backend before writing any record.
	staged := New(NewMemoryBackend(), r.installation)
	seen := map[string]bool{}
	for _, d := range data.Documents {
		key := d.Kind + "/" + d.Name
		if seen[key] {
			return protocol.Fail(protocol.Invalid, "duplicate backup document")
		}
		seen[key] = true
		validKind := false
		for _, k := range Kinds {
			validKind = validKind || k == d.Kind
		}
		if !validKind {
			return protocol.Fail(protocol.Invalid, "unknown backup kind")
		}
		if _, e = staged.backend.Create(ctx, d); e != nil {
			return e
		}
	}
	if _, e = staged.Volumes(ctx); e != nil {
		return e
	}
	if _, e = staged.Nodes(ctx); e != nil {
		return e
	}
	ops, e := staged.Operations(ctx)
	if e != nil {
		return e
	}
	issuer, e := staged.Issuer(ctx)
	if e != nil {
		return e
	}
	if !issuer.Status.Frozen {
		return protocol.Fail(protocol.Invalid, "backup writers were not frozen")
	}
	for _, d := range data.Documents {
		if d.Kind == KindFence {
			f, e := staged.fence(d)
			if e != nil {
				return e
			}
			if f.Spec.BindingID != "" {
				if _, e = staged.Node(ctx, f.Spec.BindingID); e != nil {
					return e
				}
			}
		}
	}
	for _, op := range ops {
		if op.Spec.VolumeRecordID != "" {
			if _, e = staged.Volume(ctx, op.Spec.VolumeRecordID); e != nil {
				return e
			}
		}
		if op.Spec.NodeBindingID != "" {
			if _, e = staged.Node(ctx, op.Spec.NodeBindingID); e != nil {
				return e
			}
		}
	}
	for _, d := range data.Documents {
		if d.Kind == KindIssuer {
			continue
		}
		existing, e := r.backend.Get(ctx, d.Kind, d.Name)
		if e == nil {
			if !equalJSON(existing.Spec, d.Spec) || !equalJSON(existing.Status, d.Status) {
				return protocol.Fail(protocol.Conflict, "restore would overwrite existing state")
			}
			continue
		}
		if !is(e, protocol.NotFound) {
			return e
		}
		d.UID = ""
		d.Version = ""
		if _, e = r.backend.Create(ctx, d); e != nil {
			return e
		}
	}
	// Keep freeze asserted and the backed-up issuer descriptor. A new issuer still
	// needs host termination proof; restore is not an epoch reset.
	current, e := r.Issuer(ctx)
	if e != nil {
		return e
	}
	issuer.Status.RestoreNeedsFence = current.Status.RestoreNeedsFence || issuer.Status.RestoreNeedsFence || !current.Status.Current.Valid() && issuer.Status.Current.Valid()
	if current.Status.Current.Valid() {
		issuer.Status.Current = current.Status.Current
		for _, proof := range current.Status.Prior {
			found := false
			for _, old := range issuer.Status.Prior {
				found = found || old == proof
			}
			if !found {
				issuer.Status.Prior = append(issuer.Status.Prior, proof)
			}
		}
	}
	issuer.Meta = current.Meta
	for _, q := range current.Status.RecoveryRequests {
		found := false
		for _, old := range issuer.Status.RecoveryRequests {
			if old.Nonce == q.Nonce {
				if old != q {
					return protocol.Fail(protocol.Conflict, "backup maintenance audit differs")
				}
				found = true
			}
		}
		if !found {
			issuer.Status.RecoveryRequests = append(issuer.Status.RecoveryRequests, q)
		}
	}
	issuer.Status.Frozen = true
	if _, e = r.SaveIssuer(ctx, issuer, current.Version); e != nil {
		return e
	}
	r.audited = false
	return nil
}
