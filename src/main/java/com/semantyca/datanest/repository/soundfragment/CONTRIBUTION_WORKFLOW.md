# Contribution Workflow — datanest

A **contribution** is a sound fragment submitted by an artist for a station owner to review —
either through the AI chatbot (`jesoos`) or the public, unauthenticated web form (`mixdeck`
`/submission`). Both entry points create a normal row in `mixpla__sound_fragments` tagged
`source = CONTRIBUTION`, awaiting the station owner's approval.

> This is a different system from station-to-station **sharing** — see `SHARING_WORKFLOW.md` in
> this same directory. The two are unrelated tables/enums that happen to converge in the same FE
> "received" inbox; read both if you're touching that page.

---

## 0. Why this exists (vs. sharing)

Contribution answers: **"should this brand-new song be allowed onto the platform at all?"** The
submitter isn't already part of the system (often no account yet) — this is the gatekeeping step for
unvetted content entering datanest for the first time, decided by one station owner.

Sharing (see `SHARING_WORKFLOW.md`) answers a different question entirely: **"do I also want a copy of
this song, which another station already owns and vetted, in my own library?"** Both sides there are
already trusted station owners; nothing new is entering the platform, an existing song is just being
copied between libraries. That's why it's a different table/enum/rules — it's not "the same kind of
approval, twice," it's two different real-world decisions that happen to both feel like "something is
waiting for my approval" from a station owner's point of view, which is the only reason they're merged
in the FE "received" inbox at all.

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
4. `createFromBulkUpload` sets `source = CONTRIBUTION`, `status = LifecycleStatus.NOT_APPROVED` (11),
   and threads `PublicSubmissionMetaDTO` (submitter email, artist name, description) through the whole
   chunk-assembly chain instead of piling up individual parameters.

### `requiresApproval` — one method, two callers

`createFromBulkUpload` is shared with `SoundFragmentBulkUploadController` (the **authenticated**
station-owner bulk-upload feature — a different, unrelated flow: self-owned content, no review
needed). `FileUploadService` derives `requiresApproval` from which `controllerKey` called it
(`"public-submissions"` vs `"sound-fragments-controller"`), so:

- `requiresApproval = false` (authenticated bulk upload): unchanged original behavior — `USER_UPLOAD`
  source, `status = 1` (an unrelated legacy value, not part of `LifecycleStatus`), no RLS grants beyond
  the acting user.
- `requiresApproval = true` (public submission): `CONTRIBUTION` source, `LifecycleStatus.NOT_APPROVED`,
  and the RLS grants below.

**Never** change this method's behavior without checking both call sites — a blanket status/source
change here would silently break the authenticated station owner's own uploads.

---

## 2. Silent account resolution (no separate "claim" step)

`SoundFragmentService.resolveSubmitterGrant(email)` mirrors `ListenerService.ensureUserExists` (used
for the chat-listener flow): resolve-or-create a real `core` user for the submitter's OTP-verified
email, then grant that id RLS access immediately, alongside:

- the target brand's **owner + co-owners** (`buildRlsActionsWithCoOwners`, already existed for the
  authenticated flow, reused as-is — co-owners get access automatically, no separate handling needed)
- `SuperUser`

If the submitter later registers for real via Keycloak with the same email, `core`'s own
email-lookup-by-account reuses this id — they land with access to everything they ever submitted, with
no separate "claim my submissions" feature needed.

**Station is required** (both client-side and server-side `400`) — without one, `brandIds` is empty and
nobody gets RLS access to the fragment at all; it would be created but permanently invisible.

---

## 3. Status lifecycle (`LifecycleStatus`, `com.semantyca.core.model.cnst`)

| Value | Meaning |
|---|---|
| `11` `NOT_APPROVED` | awaiting review |
| `12` `APPROVED` | reviewed, accepted |
| `13` `REJECTED` | reviewed, rejected |

Set via the shared "received inbox" accept/reject endpoints — see `SHARING_WORKFLOW.md` §3 for the
dispatch mechanism (`SharedSoundFragmentService.acceptShareByReceiver`/`rejectShareByReceiver` try the
share table first, fall back to `SoundFragmentRepository.approvePendingFragment`/`rejectPendingFragment`
for contributions). The fallback only flips status if it's currently `11` — already-actioned items are
a no-op (404 upstream).

**Status is a property of the fragment itself, not of any one brand relationship.** If a
`CONTRIBUTION`-sourced fragment ends up associated with more than one brand (e.g. it was later shared —
see `SHARING_WORKFLOW.md`), approving it anywhere makes it eligible everywhere it's linked, not just for
whoever clicked Approve.

---

## 4. Where status is (and isn't) enforced

| Query | Enforces status? |
|---|---|
| `SoundFragmentRepository.getPendingApprovalList`/`Count`/`findPendingApprovalById` (received queue) | No — shows **every** status (`source = CONTRIBUTION` is the scope, not status), so pending/approved/rejected all stay visible with the FE showing status as a tag. Mirrors how station shares already show regardless of `ApprovalStatus`. |
| `SoundFragmentBrandRepository.findForBrandFlat`/`findForBrandCount` (station's regular library page) | Yes — `AND (t.source != 'CONTRIBUTION' OR t.status = 12)`. Scoped specifically to `CONTRIBUTION` so regular self-uploads (unrelated status semantics) are never affected. |
| `jesoos`'s equivalent playback-agenda query | **Known gap, not yet fixed** (out of scope for this repo) — only excludes `NOT_APPROVED`, not `REJECTED`, and isn't scoped by `source`. A rejected contribution can currently still be selected for live on-air playback. Flagged as a follow-up task; needs the same `(source != 'CONTRIBUTION' OR status = 12)` pattern applied there. |

If you add a **new** query that lists or selects sound fragments for a brand, check whether it needs
this same `CONTRIBUTION`-scoped status gate — it's easy to recreate this gap by copying an existing
unfiltered query.

---

## Key files

| Area | File |
|---|---|
| Public submission entry point | `rest/PublicSongSubmissionController.java` |
| OTP verification | `service/OtpService.java` |
| Chunk assembly / dispatch | `service/util/FileUploadService.java` |
| Fragment creation, RLS, submitter resolution | `service/soundfragment/SoundFragmentService.java` (`createFromBulkUpload`, `resolveSubmitterGrant`) |
| Submission metadata carrier | `dto/PublicSubmissionMetaDTO.java` |
| Received-queue / approve-reject queries | `repository/soundfragment/SoundFragmentRepository.java` (`getPendingApprovalList`, `approvePendingFragment`, `rejectPendingFragment`) |
| Regular library status gate | `repository/soundfragment/SoundFragmentBrandRepository.java` (`findForBrandFlat`) |
| Chatbot entry point (other repo) | `jesoos` — `service/chat/tools/UploadSongToolHandler.java`, and that repo's `CHAT_WORKFLOW.md` |
