# Attached-only provider profile, 20 November 2026

Version: 0.1.0-rc.3. This is a restricted RWO/ext4 deployment profile;
`productionReady` remains false and unrestricted CSI conformance is not claimed.

Fresh direct API expansion of an unattached empty volume still ended in ERROR.
The driver therefore rejects actual growth before creating an operation or
sending a native POST unless the durable registered attachment, learned serial,
active native VM and IN_USE volume agree. It locks both node and volume and
checks the attachment and the agent’s complete guest serial inventory again after
claiming them. Uncertain growth reconciliation retains both claims unless the
saved binding, live VM, provider attachment and guest inventory still agree;
legacy operations without a node binding keep read-only reconciliation. Satisfied capacity remains
idempotent without a native write.

Candidate binary and chart were exercised on a permanent five-node RKE2 cluster
with two worker agents. The following checks passed with synthetic data:

- A mounted PVC grew from10Gi to12Gi while its writer continued without a pod
  restart. All8MiB of payload were independently read and SHA256 verified.
- A detached10Gi PVC requesting12Gi received FailedPrecondition. Native size
  stayed10 and no extend operation was recorded.
- The same PVC was attached on the other worker and then grew to12Gi. Payload,
  filesystem UUID, learned serial and inode were preserved.
- The external resizer's default in-use handling stalled the second case after
  attachment. Setting `--handle-volume-inuse-error=false` allowed the existing
  request to retry and complete while the writer was running.

The candidate used a read-only binary overlay on the previous released runtime.
This proves the candidate code and rendered resizer behavior, not the identity
of an unpublished final container. Exact published image verification and final
fixture cleanup are a separate deployment acceptance step.

These results do not supersede the19 November full rc.2 suite, whose two
failures were native detached expansion. No new successful full conformance run,
provider repair, lost-node recovery or application-data migration is claimed.
Lost-node movement continues to require explicit old-VM fencing. Keep Retain,
WaitForFirstConsumer and backups of the registry plus application data. Never
manually detach or move a provider disk during driver operations.

## Published runtime verification

The immutable published rc.3 image was subsequently verified on the controller
and both worker agents. The candidate overlays were removed. Binary SHA256
matched the anonymously downloaded release binary on all three pods.

Both existing PVCs then grew from12Gi to14Gi, one on each worker, without
restarting their writers. All8MiB payloads were independently read and checked;
inode, serial and filesystem UUID were preserved. Deleting each PVC under Retain
left its released PV and detached native disk intact. Only these synthetic PVs
were then explicitly switched to Delete and removed through CSI. Both native
disks and the temporary namespace were confirmed absent.

See [sanitized published-runtime evidence](published-profile-2026-11-20.json) for
the immutable image and binary digests. The restricted profile passed these
checks; general production readiness and full conformance remain unapproved.
