# Identity and installation

This public release uses CSI identity and node labels under `clo.csi.rave-technology.io`
and the matching CRD API group. Install it as a new installation. An installation
using different identities or API groups cannot be upgraded by renaming CRDs,
PV objects, labels or registry records. Preserve backups and use an explicit data
migration procedure; this prerelease does not implement such an identity migration.
