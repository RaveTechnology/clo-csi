EXPERIMENTAL / NOT PRODUCTION READY.

rc.5 closes an exact never-dispatched Prepared operation even when interruption
left only one of its two claims acquired. Closing uses the operation's CAS before
releasing only claims still naming that operation. Dispatch still requires both
claims; sent operations continue to require observed postconditions or explicit
VM fencing. Aborting preparation uses a separate bounded cancellation context,
so an expired caller cannot strand a claim during cleanup. Recovery rejects live
preparers through dispatch metadata writes; cancelled cleanup retains issuer and
restore-fence checks while allowing ordinary maintenance drain. No registry reset or
native mutation replay is introduced. The chart gives controller and node agents
the Kubernetes system priorities needed to schedule before their PVC consumers.
Live bootstrap acceptance of this release is pending.

rc.4 gives node HTTPS requests a three-minute budget for fresh cloud proofs.
Independent nodes share the controller's one-request-per-second CLO API queue;
the previous45-second limit could expire valid stage/report requests during a
parallel bootstrap. A regression test exercises eight independent node clients
against that shared API pace. Caller cancellation still bounds shorter requests.
Fresh cloud verification, credentials and operation authority remain required.
The wire protocol is compatible with rc.3, allowing node agents to roll first
while the existing controller retains its issuer and pending operation records.

rc.3 rejects actual growth unless CLO reports IN_USE on the volume's registered,
active node. Detached growth returns FailedPrecondition before creating an
operation or sending a native POST. Attach the PVC to a running pod before
retrying. Already satisfied capacity remains idempotent without a cloud write.
Growth holds both node and volume claims and rechecks attachment plus the agent’s
live guest serial inventory before dispatch. Recovery of a bound extend requires
the same attachment and inventory before releasing either claim; uncertain native
requests are never resent.
Do not detach a disk through the cloud console while it is being expanded.
The resizer uses --handle-volume-inuse-error=false so a detached precondition
failure can be retried after attachment instead of waiting for the pod to stop.

This is an attached-only provider profile, not unrestricted CSI conformance or
a provider fix. CSI ONLINE permits requests at any lifecycle stage after create;
detached growth is deliberately rejected here because CLO corrupts its state.
The full rc.2 suite on19 November had50PASS/2FAIL/50SKIP/1pending, with both failures
on fresh detached expansion; separate attached-PVC growth passed. Live rc.3
profile acceptance passed online growth, detached rejection and cross-node
reattachment followed by growth; see docs/attached-profile-2026-11-20.md.
Exact published runtime verification is a separate deployment step.

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
