EXPERIMENTAL / NOT PRODUCTION READY.

rc.3 rejects actual growth unless CLO reports IN_USE on the volume's registered,
active node. Detached growth returns FailedPrecondition before creating an
operation or sending a native POST. Attach the PVC to a running pod before
retrying. Already satisfied capacity remains idempotent without a cloud write.
Growth holds both node and volume claims and rechecks attachment before dispatch.
Do not detach a disk through the cloud console while it is being expanded.

This is an attached-only provider profile, not unrestricted CSI conformance or
a provider fix. CSI ONLINE permits requests at any lifecycle stage after create;
detached growth is deliberately rejected here because CLO corrupts its state.
The full rc.2 suite on19 November had50PASS/2FAIL/50SKIP/1pending, with both failures
on fresh detached expansion; separate attached-PVC growth passed. Live rc.3
profile acceptance is recorded separately from those historical results.

Includes the rc.2 fix for typed operation records: the CloOperation CRD preserves
apiVolumeIDs:null instead of pruning it and invalidating the immutable intent.
Apply the updated CRDs before upgrading the controller; Helm upgrade alone does
not update CRDs. Previously pruned initialization is recovered only when its
original v1 hash proves this exact missing null field. UID, stored spec and
commitment remain unchanged; used or lost sealed status is never reset.

Controller expansion also rejects reported target capacity while the provider
volume is in ERROR or a transitional state, including after a competing grow.
These checks do not resend the native expansion request.

Fresh detached-volume expansion still transitions to ERROR in CLO's direct API.
No new successful full native conformance run is claimed. RWO/ext4 only. Includes the binary,
Helm chart, exact container image digest and checksums. Publication is not storage
production acceptance. Apache-2.0.
