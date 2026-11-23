package coordinator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"github.com/RaveTechnology/clo-cloud-go/cloud"
	"github.com/RaveTechnology/clo-csi/internal/csi/protocol"
	"github.com/RaveTechnology/clo-csi/internal/csi/registry"
	"github.com/google/uuid"
	"math"
)

const GiB int64 = 1 << 30

func actualCapacity(gb int, limit int64) error {
	if gb <= 0 || gb > int(math.MaxInt64/GiB) || limit > 0 && int64(gb)*GiB > limit {
		return protocol.Fail(protocol.RecoveryRequired, "provider capacity is invalid or exceeds the saved upper bound")
	}
	return nil
}

func capacity(required, limit int64) (int, int64, error) {
	if required < 0 || limit < 0 || limit > 0 && required > limit {
		return 0, 0, protocol.Fail(protocol.Invalid, "invalid capacity range")
	}
	if required == 0 {
		required = 10 * GiB
	}
	gb := (required-1)/GiB + 1
	if gb > math.MaxInt64/GiB || gb > int64(math.MaxInt) {
		return 0, 0, protocol.Fail(protocol.Invalid, "capacity overflows reported bytes")
	}
	bytes := gb * GiB
	if limit > 0 && bytes > limit {
		return 0, 0, protocol.Fail(protocol.Invalid, "rounded capacity exceeds limit")
	}
	return int(gb), bytes, nil
}
func fingerprint(q protocol.VolumeRequest) string {
	h := sha256.Sum256([]byte(q.FSType + ":" + q.AccessMode))
	return hex.EncodeToString(h[:])
}
func (c *Coordinator) EnsureVolume(ctx context.Context, q protocol.VolumeRequest) (registry.VolumeRecord, error) {
	done, admitErr := c.begin(ctx)
	if admitErr != nil {
		return registry.VolumeRecord{}, admitErr
	}
	defer done()
	if q.Name == "" || q.FSType != "ext4" || q.AccessMode != "SINGLE_NODE_WRITER" {
		return registry.VolumeRecord{}, protocol.Fail(protocol.Invalid, "only named ext4 SINGLE_NODE_WRITER volumes are supported")
	}
	gb, _, e := capacity(q.RequiredBytes, q.LimitBytes)
	if e != nil {
		return registry.VolumeRecord{}, e
	}
	if e = c.checkIssuer(ctx); e != nil {
		return registry.VolumeRecord{}, e
	}
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	name := registry.Name("v", q.Name)
	v, e := c.repo.Volume(ctx, name)
	if e == nil {
		if v.Spec.CSIName != q.Name || v.Spec.Fingerprint != fingerprint(q) || v.Status.Retired || v.Status.Deleted {
			return v, protocol.Fail(protocol.Exists, "CSI name belongs to an incompatible/retired volume")
		}
		if pending, e := c.pending(ctx, v, "create"); pending {
			if e != nil {
				return v, e
			}
			v, e = c.repo.Volume(ctx, name)
			if e != nil {
				return v, e
			}
		}
		if v.Status.ProviderID != "" {
			a, e := c.ownedVolume(ctx, v)
			if e != nil {
				return v, e
			}
			bytes := int64(a.SizeGB) * GiB
			if a.SizeGB <= 0 || a.SizeGB > int(math.MaxInt64/GiB) || bytes < q.RequiredBytes || q.LimitBytes > 0 && bytes > q.LimitBytes {
				return v, protocol.Fail(protocol.Exists, "existing volume does not satisfy capacity range")
			}
			return v, nil
		}
	} else if !code(e, protocol.NotFound) {
		return v, e
	}
	if e = c.audit(ctx); e != nil {
		return v, e
	}
	actual, e := c.inventory(ctx)
	if e != nil {
		return v, e
	}
	for _, a := range actual {
		if a.Name == c.repo.ProviderName(q.Name) {
			return v, protocol.Fail(protocol.Exists, "provider name exists without a dispatched owned intent")
		}
	}
	if v.Name == "" {
		v, e = c.repo.CreateVolume(ctx, registry.VolumeSpec{CSIName: q.Name, Fingerprint: fingerprint(q), RequiredBytes: q.RequiredBytes, LimitBytes: q.LimitBytes, FSType: q.FSType, AccessMode: q.AccessMode, PVCUID: q.PVCUID})
		if e != nil {
			return v, e
		}
	}
	op, finishPreparation, e := c.newOperation(ctx, "create", v, nil, protocol.Snapshot{}, nil, gb, q.LimitBytes)
	defer finishPreparation()
	if e != nil {
		return v, e
	}
	id := ""
	e = c.send(ctx, op, func() error {
		var err error
		id, err = c.api.CreateVolume(ctx, c.cfg.ProjectID, cloud.CreateVolumeRequest{Name: v.Spec.ProviderName, SizeGB: gb})
		return err
	})
	if e != nil {
		return v, e
	}
	if e = c.operationAuthority(ctx, op); e != nil {
		return v, e
	}
	if _, e = uuid.Parse(id); e != nil {
		c.uncertain(ctx, op, "invalid create response ID")
		return v, protocol.Fail(protocol.RecoveryRequired, "create returned invalid ID; inspect %s", op.Name)
	}
	v, e = c.repo.Volume(ctx, v.Name)
	if e != nil {
		return v, e
	}
	v.Status.ProviderID = id
	v.Status.FSUUID = id
	v, e = c.repo.SaveVolume(ctx, v, v.Version)
	if e != nil {
		return v, e
	}
	actualVolume, e := c.waitVolume(ctx, v, func(a cloud.Volume) bool {
		return a.Status == "AVAILABLE" && a.AttachedServerID == "" && a.SizeGB >= gb
	})
	if e != nil {
		c.uncertain(ctx, op, "create postcondition not established")
		return v, protocol.Fail(protocol.RecoveryRequired, "create postcondition unresolved: %s", op.Name)
	}
	if e = actualCapacity(actualVolume.SizeGB, q.LimitBytes); e != nil {
		c.uncertain(ctx, op, "provider capacity outside saved range")
		return v, e
	}
	v.Status.SizeGB = actualVolume.SizeGB
	v, e = c.repo.SaveVolume(ctx, v, v.Version)
	if e != nil {
		return v, e
	}
	if e = c.finish(ctx, op, id); e != nil {
		return v, e
	}
	return c.repo.Volume(ctx, v.Name)
}
