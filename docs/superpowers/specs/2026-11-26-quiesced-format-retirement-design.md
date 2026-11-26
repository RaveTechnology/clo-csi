# Retire unused interrupted initialization on a live node

An interrupted first format can retain the volume and node claims even when its
node is healthy. The existing fenced retirement remains the recovery path for a
lost node. Add a separate, explicit admin action, `retire-quiesced-format`, for an
unused incomplete volume whose live agent can durably revoke old permits.

The operator selected this approach and delegated implementation on 2026-11-26.
This is not automatic CSI recovery. It does not stop a VM, discard a filesystem,
authorize another format, delete a disk, reset an epoch, or alter lost-node
fencing. The original disk remains permanently retired and retained.

## Approaches

Fenced retirement and worker replacement use existing behavior but affect every
workload on that worker. Direct state resets or another mkfs are unacceptable.
The selected live path uses the same authenticated per-volume quiescence that
already permits normal hot Unpublish; the node identity and kernel proof remain
mandatory. Review found ordinary quiesce cannot prove absence of a raw signature. Add
probeBlank and quiesceBlank with affirmative blankChecked receipts. The selected
recovery node agent must be upgraded as well as the controller; old agents refuse
these jobs. Ordinary quiesce and normal jobs remain unchanged.

## Admission and proof

Require exact installation, full CSI handle, format operation identity, current
or proven-gone issuer, and an inactive dispatched first-format operation. The
volume must still name that operation and node binding, with initialization
`FormatDispatched`. Initialized volumes, active preparers, unknown issuers,
restored epochs needing fencing, foreign attachments and mismatched identities
are refused. The live node must be unfenced, unquarantined, and on its exact
active VM; a fence ID is not accepted by this action.

Before mutation, collect a raw read-only probeBlank from the upgraded authenticated agent.
Both fresh and saved receipts must match the immutable original operation boot;
current re-enrollment boot status alone is insufficient.
Verify the current boot, root/device identity, provider attachment and complete
inventory. The selected non-root, unpartitioned device must be unique, have no
mounts and have no recognized filesystem signature. An existing filesystem uses
ordinary format reconciliation or the fenced recovery path instead.

Dry run performs these read-only eligibility checks. It does not close the local
volume barrier or promise that a later commit can quiesce. Document this limit.

Before actual quiescence, persist the exact operator request on the operation.
The new node `quiesceBlank` job serializes with Stage on the persistent volume
lock, saves `Barrier` and `DetachCommitted`, refuses stage/pod/pending references,
and proves kernel-level exclusive unmounted access across mount namespaces.
While holding that lock it refuses local FormatComplete, raw signatures of any
type and probe errors, independently of possibly stale lsblk. An old captured Stage permit is blocked by that committed local barrier.

Validate the fresh quiescence receipt against the exact volume, binding, boot,
blank device and current native attachment again. Persist the exact request/job before dispatch, and the unmodified authenticated
receipt before setting a permanent retired/barrier tombstone. Seal a SHA-256 of
operation UID, intent/job and receipt in that tombstone; require the original
request audit, exact job, original boot and seal on every completion retry. Then complete the original format
operation without changing its Sent flag, nonce, format reference, serial,
filesystem identity or attachment epoch, and release only its own claims.

## Interruptions and retention

A failed or unknown quiescence never permits state reset or a second format.
Its local safety barrier may remain closed; the disk and original format intent
remain available for explicit recovery. The action must resume after a saved
receipt, tombstone, operation completion or partial claim release. A saved
tombstone is valid only with its matching persisted quiescence proof. Never
erase a later claim. Reusing a nonce with changed input is refused.

Retirement and its existing barrier are monotonic in the repository. Publish,
Stage and Delete continue to reject the retired disk. Normal hot Unpublish may
subsequently detach it, using the existing quiescence protocol. The disk is not
automatically deleted or reused. Backup/export/restore must preserve the proof,
tombstone and complete format history.

## Validation and live scope

Prove delayed and concurrent old-format execution cannot pass the local barrier;
kernel-busy devices, mounts, pending references, foreign signatures, changed
boots/bindings/issuers and initialized filesystems must fail closed. Exercise
interruption and repeated recovery, backup graph compatibility and no cloud or
filesystem mutation dispatch. Keep the existing fenced retirement tests green.

Run one independent review of the whole change before live use. Build an
immutable image with the additive jobs; update only the selected recovery node
agent and controller. Older agents refuse new jobs; normal jobs remain compatible.
Do not downgrade to a writer that drops proof fields after saving an actual intent. Before controller replacement, take a supported registry
backup and capture issuer identities. The new controller must prove the old
process Gone through the existing agent; no epoch or issuer bypass is allowed.

Live acceptance is limited to one explicitly pinned empty rehearsal disk, with
the existing source nodes, source PV identities, running SQL cohort and protected
independent production applications unchanged. Run dry first, actual once with
durable intent, then observe standard Unpublish and retained native disk state.
Do not manufacture another interrupted format for testing on shared workers.
