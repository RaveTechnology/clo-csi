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
