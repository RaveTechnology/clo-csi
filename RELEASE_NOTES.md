EXPERIMENTAL / NOT PRODUCTION READY.

Fixes creation of typed operation records: the CloOperation CRD now preserves
apiVolumeIDs:null instead of pruning it and invalidating the immutable intent.
Apply the updated CRDs before upgrading the controller; Helm upgrade alone does
not update CRDs. Previously pruned initialization is recovered only when its
original v1 hash proves this exact missing null field. UID, stored spec and
commitment remain unchanged; used or lost sealed status is never reset.

Fresh detached-volume expansion still transitions to ERROR in CLO. Full native
sanity of the released image remains pending. RWO/ext4 only. Includes the binary,
Helm chart, exact container image digest and checksums. Publication is not storage
production acceptance. Apache-2.0.
