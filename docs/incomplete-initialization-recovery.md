# Interrupted initial filesystem creation

A cancelled initial stage can leave its one-time format grant consumed while the
guest has no complete filesystem. The driver must not issue another mkfs for
that volume. This procedure preserves the original disk and its history.

An explicit `retire-incomplete-format` admin action requires the exact saved
format operation, the full CSI volume handle and a matching verified old-VM
fence. The original issuer must be current or independently proven Gone. Only
`FormatDispatched` is eligible; a completed initialization is refused. The native
volume must belong to this installation and be free or attached to that exact
fenced VM. A live preparer, foreign attachment, missing fence or restored epoch
blocks retirement.

Retirement sets a barrier and a permanent tombstone, closes the original format
operation without rewinding its Sent flag, and releases only its own claims.
Filesystem state, original nonce, learned serial and format reference remain.
No native cloud write, filesystem write, second mkfs or disk deletion occurs.
Ordinary fenced Unpublish can subsequently detach the retained disk. CSI Delete
continues to reject retired volumes. Replacement of a PVC is a separate operator
decision; the original retained PV/disk is not discarded by this action.

Initial node registration, stage authorization and filesystem receipts also use
one shared, nonblocking admission gate for fresh cloud proofs. Busy callers get
a retryable Conflict before issuing cloud reads or creating an operation.
Binding lookup, polling and job receipts bypass it so host verification and
maintenance cannot deadlock behind a proof. This bounds cloud queue pressure
during a simultaneous PVC bootstrap without weakening fresh verification or
increasing the provider request rate.

Validation must cover absence and mismatch of fences, initialized data, foreign
attachments/issuers/UIDs, dry runs, interruption and repeated retirement, retained
native data, cold detach, concurrent proof admission and an independent recovery
channel. Live acceptance uses the exact published image. General production
readiness and unrestricted CSI conformance remain separate gates.

## Healthy live node

For a healthy reachable node, the separate explicit `retire-quiesced-format`
action can retain an unused interrupted disk without stopping its VM. Lost,
unreachable or unverifiable nodes still require the fenced procedure above.
Only an inactive sent first format with `FormatDispatched`, exact current
binding/attachment and a unique unmounted non-root, unpartitioned device with
no recognized filesystem type or UUID is eligible. This proves no recognized
filesystem signature, not zero-filled media or authority to erase any data.

Use the exact saved operation and full CSI handle, explicit installation/project/
context, operator and a saved UUID nonce. Do not provide a fence ID. Inspect the
records first, then run the scoped command with `--dry-run`. A successful dry run
checks eligibility using fresh read-only facts; it does not close the local
barrier or establish kernel quiescence. Actual execution can still refuse.

Actual execution saves the operator request before requesting authenticated
quiescence. The existing node agent holds the persistent per-volume lock,
commits its local detach barrier, refuses stage/target/pending references, and
requires kernel exclusive unmounted access across mount namespaces. Captured old
format permits cannot pass this barrier. The controller rechecks the current
boot, provider attachment and absence of a filesystem, persists the exact
quiescence receipt, then permanently retires the volume and closes only the
original format's claims. It never reissues mkfs or rewinds the format intent.

An interrupted or rejected actual request may leave the local barrier closed.
Keep the original disk/history and inspect the saved outcome. Resume the same
exact request and nonce only after identifying the interrupted result; never
reset the journal or synthesize a replacement receipt. A matching saved proof
allows completion after a tombstone, completed operation or partial release;
newer unrelated claims remain intact. Normal hot Unpublish may then detach the
retained disk. Publish, Stage and Delete continue to refuse it permanently.

This controller addition uses the existing baseline/quiesce wire protocol and
requires no node-agent update from the preceding release. Upgrade only the
controller after a supported freeze/export/resume backup. Preserve the registry
epoch and require native proof that the preceding issuer process is Gone. Do not
roll node agents or manufacture another interrupted format on shared workers to
exercise this recovery.
