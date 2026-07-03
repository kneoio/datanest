# Contribution Workflow — datanest

A **contribution** is a sound fragment submitted by an artist for a station owner to review —
either through the AI chatbot (`jesoos`) or the public, unauthenticated web form (`mixdeck`
`/submission`). Both entry points create a normal row in `mixpla__sound_fragments` tagged
`source = CONTRIBUTION`, then — if a target station was specified — a **share** pointing at that
station, using the exact same mechanism as station-to-station sharing. See `SHARING_WORKFLOW.md` in
this same directory for the mechanism itself; this doc covers the contribution-specific entry points
and account resolution.

---

## 0. Why this exists (and why it's built on sharing, not a separate approval system)

A contribution answers: **"should this brand-new song be allowed onto this station's library at
all?"** The submitter often has no account yet — this is the gatekeeping step for unvetted content
from an outside artist, decided by one station owner.

This used to be its own system: a `LifecycleStatus`-driven `status` column checked directly on the
fragment, with the fragment carrying a direct brand association and RLS grant to the station from
the moment it was created. That had a real bug: a submitter who later registered a genuine `mixdeck`
account could never see their own submission again, because the fragment's existing brand
association excluded it from the one page (`/sound-library/unassigned-to-brands`) that would
otherwise show a plain registered user their own unassigned content.

The fix: route contributions through **sharing**. The fragment gets no brand association and no
direct station RLS at creation — it's visible only to the submitter's resolved account (see §2)
until a station accepts the share, exactly like an inter-station share. There is no longer a
separate approval enum or status column for this — `ApprovalStatus` (`PENDING`/`ACCEPTED`/`REJECTED`,
see `SHARING_WORKFLOW.md` §1) is the only status contributions have.

---

## 1. Entry points

| Entry point | Where | Auth |
|---|---|---|
| AI chatbot | `jesoos` — `service/chat/tools/UploadSongToolHandler` (not in this repo) | authenticated listener with the `artist` label |
| Public web form | `rest/PublicSongSubmissionController` (`/datanest/public/songs/*`) | anonymous, gated by an emailed OTP code |

Both eventually call `SoundFragmentService.createFromBulkUpload(...)` (jesoos calls it indirectly via
its own upsert path; the web form goes through `FileUploadService`'s chunk-assembly pipeline). Only
the **public web-form path** is documented in detail here — the chatbot path lives in the `jesoos`
repo's own `CHAT_WORKFLOW.md`.

### Public web-form flow (`PublicSongSubmissionController` → `FileUploadService` → `SoundFragmentService`)

1. `POST /public/songs/request-code` — `OtpService.sendOtp(email)` generates a 6-digit code (10 min
   TTL, in-memory `ConcurrentHashMap`), emails it. **Not consumed on success** — `isVerifyFail` stays
   valid until it naturally expires, because it's re-checked on *every chunk* of a multi-chunk upload
   and again for "submit another track" (a single-use code would break both).
2. A fixed QA bypass exists: email `qa-test@mixpla.io` + code `424242` always verifies, bound to that
   one address only — cannot be used to skip verification on a real submission.
3. `POST /public/songs/chunk` (used by the FE) — `stationSlug` and `email`/`code` are required on
   every chunk request (`400`/`401` otherwise). Chunks stream to disk; on the last chunk,
   `FileUploadService.assembleAndProcess` extracts audio metadata (FFprobe via `AudioMetadataService`)
   and calls `SoundFragmentService.createFromBulkUpload(..., requiresApproval=true, meta)`.

### `requiresApproval` — one method, two callers

`createFromBulkUpload` is shared with `SoundFragmentBulkUploadController` (the **authenticated**
station-owner bulk-upload feature — a different, unrelated flow: self-owned content, no review
needed). `FileUploadService` derives `requiresApproval` from which `controllerKey` called it
(`"public-submissions"` vs `"sound-fragments-controller"`), so:

- `requiresApproval = false` (authenticated bulk upload): unchanged original behavior — `USER_UPLOAD`
  source, direct brand association, no share involved.
- `requiresApproval = true` (public/chat submission): `CONTRIBUTION` source, **no** brand association
  at creation (`brandIds = List.of()`), RLS granted only to the submitter's resolved account (§2) and
  `SuperUser` directly on the fragment — and, when a target station was specified, a `PENDING` share
  is created via `SharedSoundFragmentService.shareContribution(...)` right after insert.

**Never** change this method's behavior without checking both call sites — a blanket change here
would silently affect the authenticated station owner's own uploads.

Both paths set `fragment.setStatus(1)` — a fixed placeholder value, not `LifecycleStatus` and not
`ApprovalStatus`. The fragment itself no longer carries an approval status; the share does.

---

## 2. Silent account resolution (no separate "claim" step)

`SoundFragmentService.resolveSubmitterAccount(email)` mirrors `ListenerService.ensureUserExists` (used
for the chat-listener flow): resolve-or-create a real `core` user for the submitter's OTP-verified
email, then grant that id RLS access on the fragment directly, alongside `SuperUser`
(`userGrant(userId)`).

If the submitter later registers for real via Keycloak with the same email, `core`'s own
email-lookup-by-account reuses this id — they land with access to everything they ever submitted
(via direct fragment RLS, and via the shares those submissions created), with no separate "claim my
submissions" feature needed.

**Station is optional at the API level**, but if none is given, no share is created — the fragment is
still visible to the submitter's own resolved account but no station will ever see it. In practice
`mixdeck`'s public submission form requires a station be chosen before allowing submission.

---

## 3. Status lifecycle

Contributions no longer have their own status enum or column — they use `ApprovalStatus` on the
share created for them, exactly as described in `SHARING_WORKFLOW.md` §1. Accept/reject go through
the same `SharedSoundFragmentService.acceptShareByReceiver`/`rejectShareByReceiver` →
`SharedSoundFragmentRepository.acceptByReceiver`/`rejectByReceiver` used for station-to-station
shares (`SHARING_WORKFLOW.md` §2) — accepting grants the station RLS on the fragment and creates the
brand association; rejecting does neither.

If a `CONTRIBUTION`-sourced fragment is shared to more than one station (e.g. the submitter also sent
it directly, or a station that received it re-shares it onward), each target station has its own
independent share/status — accepting or rejecting for one station has no effect on another's.

---

## 4. Where status is (and isn't) enforced

| Query | Enforces status? |
|---|---|
| `SharedSoundFragmentRepository.getReceivedList`/`getReceivedListCount` (received queue) | No — shows every share regardless of `ApprovalStatus`, the FE renders status as a tag. |
| `SoundFragmentBrandRepository.findForBrandFlat`/`findForBrandCount` (station's regular library page) | Implicitly — a fragment only appears here once `mixpla__brand_sound_fragments` has a row for that brand, which only happens on share acceptance. No separate status check is needed in this query anymore (the earlier `CONTRIBUTION`-specific status gate here was removed as part of this redesign — brand association itself is now sufficient gating). |
| `jesoos`'s playback-agenda query | Out of scope for this repo. If it still special-cases `source = CONTRIBUTION` + a `LifecycleStatus` value, that check is now obsolete under this design and should be reverted — a `CONTRIBUTION` fragment is eligible for playback exactly when it has a `mixpla__brand_sound_fragments` row, same as any other fragment, with no extra status gate needed. |

If you add a **new** query that lists or selects sound fragments for a brand, you do not need a
contribution-specific status gate — brand association (created only on share acceptance) is already
the correct scope.

---

## Key files

| Area | File |
|---|---|
| Public submission entry point | `rest/PublicSongSubmissionController.java` |
| OTP verification | `service/OtpService.java` |
| Chunk assembly / dispatch | `service/util/FileUploadService.java` |
| Fragment creation, submitter resolution, share creation trigger | `service/soundfragment/SoundFragmentService.java` (`createFromBulkUpload`, `resolveSubmitterAccount`) |
| Submission metadata carrier | `dto/PublicSubmissionMetaDTO.java` |
| Share creation for contributions | `service/soundfragment/SharedSoundFragmentService.java` (`shareContribution`) |
| Chatbot entry point (other repo) | `jesoos` — `service/chat/tools/UploadSongToolHandler.java`, and that repo's `CHAT_WORKFLOW.md` |
