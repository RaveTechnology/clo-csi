# Quiesced format retirement implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Retire one unused incomplete format on a verifiably quiesced live node without VM fencing or another mkfs.

**Architecture:** A separate explicit admin action reuses authenticated baseline/quiesce jobs, persists their proof before the retained tombstone, and resumes only its own interrupted metadata updates. Existing lost-node fencing and ordinary quiesce remain unchanged; upgrade the selected recovery agent.

**Tech Stack:** Go, existing CSI coordinator/registry/admin CLI, Kubernetes and CLO native read-only postcondition checks.

**Spec:** `docs/superpowers/specs/2026-11-26-quiesced-format-retirement-design.md`

## Global constraints

- Explicit admin action only; no automatic retirement, VM stop, second mkfs, native disk deletion or metadata reset.
- Use additive raw probeBlank/quiesceBlank jobs and exact active VM/binding/boot/device proof.
- Keep `FormatDispatched`, Sent, nonce, serial, FSUUID, attachment epoch and original disk history.
- Persist matching quiescence proof before permanent tombstone; never release another operation's claims.
- Source nodes/PVs, source SQL cohort and independent production namespaces remain unchanged in live acceptance.
- Public code/docs contain no real installation identities, credentials, product names or private repository hosts.
- Execute inline, then one independent whole-change review; no review per task.

## Review focus

- A delayed format permit, including a formatter already holding the local journal lock, cannot execute after quiescence.
- A changed boot, foreign mount namespace, missing kernel proof or duplicate device never admits retirement.
- Interruption between proof, tombstone, completion and release cannot reopen the format grant or erase newer claims.
- A fabricated/mismatched saved receipt or an already initialized volume cannot use the new recovery path.
- Existing fenced recovery, backup graph validation and unchanged node/controller wire compatibility remain intact.

### Task 1: Coordinator retirement and durable proof

**Files:** `internal/csi/coordinator/retire_quiesced_format.go`, coordinator tests, `internal/csi/coordinator/recovery.go`, registry monotonic-state tests/guard, node runtime tests.

**Interfaces:** Consume `Recover`, `stageFacts`, `exchange`, `operationAuthority`, persistent node quiescence, registry CAS and exact claim release. Produce `retire-quiesced-format` returning the completed original operation with retained disk/history and saved quiescence proof.

- [ ] Add failing coordinator tests for active unused interrupted format, dry-run immutability, initialized/foreign/unreachable/fenced/busy/malformed cases, exact saved proof and interruption/retry.
- [ ] Add failing node regressions for captured permits and concurrent Stage/quiesce; require kernel proof across namespaces.
- [ ] Add failing repository tests for reopening retirement/barrier, and an export/restore graph regression.
- [ ] Run focused tests and observe RED due missing action/guard; preserve logs in the plan workspace.
- [ ] Implement the separate coordinator action, fresh baseline/blank-device checks, durable request/receipt/tombstone and restart-safe exact release. Retain old fenced action.
- [ ] Run focused tests, then `go test -race ./...` and `go vet ./...`; expect all PASS. Commit only this task's files.

### Task 2: Admin surface and documentation

**Files:** `cmd/clo-csi/main.go`, `cmd/clo-csi/admin.go`, command/admin tests, `docs/incomplete-initialization-recovery.md`, README as needed.

**Interfaces:** Consume Task 1 recovery action. Produce explicit scoped CLI/local admin dispatch with full handle and nonce; old commands remain unchanged.

- [ ] Add failing CLI/admin dispatch, scope mismatch, unknown input and dry-run message tests.
- [ ] Add the action without a new remote/public endpoint or node protocol. Explain read-only dry-run limits, local barrier on interrupted apply and permanent retained disk.
- [ ] Run command tests and full race/vet; expect PASS. Check public-content hygiene and diff whitespace. Commit.

### Task 3: Whole-change review and bounded live acceptance

**Files:** private operator artifacts outside public source; reviewed controller build/profile docs only contain synthetic information.

**Interfaces:** Consume committed Tasks 1–2. Produce reviewed immutable controller artifact, supported issuer handoff and one live retained-disk receipt; no production cutover authority.

- [ ] Review the whole diff from the plan base in one fresh context. Fix Critical/Important once with RED→GREEN tests and green suite.
- [ ] Build immutable controller image; record exact source/image hashes and additive fail-closed protocol and exact selected-node image.
- [ ] Collect fresh read-only target/source identities, sole pending operation, protected workload health and registry epoch/issuer. Take supported formal freeze/export/resume backup.
- [ ] Patch only the selected agent Pod driver image through UID/spec CAS; preserve other agent Pod images/UIDs and DaemonSet spec. Replace the controller image through UID/spec CAS. Observe exact new issuer and proven old process termination without epoch reset; preserve all source node/PV/SQL identities.
- [ ] Dry-run only the pinned unused rehearsal volume. After accepted eligibility, save one durable actual intent and retire it once through supported API.
- [ ] Observe normal hot Unpublish, native AVAILABLE retained disk, closed original operation and idle node enrollment. Never replay an unknown actual outcome or delete the retained disk.
- [ ] Verify original source node/PV/SQL cohort, independent production HTTP/replicas and supplemental cold CSI admission. Record remaining migration gates; do not claim full migration readiness.
