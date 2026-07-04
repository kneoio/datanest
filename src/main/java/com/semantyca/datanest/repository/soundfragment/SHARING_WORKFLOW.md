# Sharing Workflow — datanest

A **share** is one entity — a station, or an outside artist contributing a song — offering a sound
fragment to a target station, who can accept or reject it. Everything lives in
`mixpla__shared_sound_fragments` / `SharedSoundFragment`, tracked by one `ApprovalStatus` enum.
Station-to-station sharing and artist contributions are **the same mechanism**; this doc covers both.

---

## 0. Why this exists

The fragment being offered gets **no brand association and no direct RLS to the target station at
creation time**. Visibility for the target station comes entirely from the share row — but "no RLS"
only means no RLS **on the fragment**. There are two separate RLS layers in play, on two separate
tables, granted at two separate times:

| Layer | Table | Granted to | Granted when |
|---|---|---|---|
| Share-entity RLS | `mixpla__shared_sound_fragment_readers` (entity_id = the `SharedSoundFragment` row) | target brand's **owner** + `SuperUser` (`insertRlsForReceivers`) | immediately at share creation |
| Fragment RLS | `mixpla__sound_fragment_readers` (entity_id = the `SoundFragment` row) | target brand's owner **+ co-owners** (`grantFragmentRlsToBrand`) | only on **accept** |

The share-entity RLS is what makes the offer show up in the target station owner's `/received` inbox
at all, and is what `acceptByReceiver`/`rejectByReceiver` check (`... id IN (SELECT entity_id FROM
mixpla__shared_sound_fragment_readers WHERE reader = $2)`) before allowing a decision — a station
owner who was never granted a reader row on that specific share cannot act on it. It says nothing
about whether the underlying song itself is visible anywhere else (brand library, playback, etc.) —
that's the fragment-RLS layer, untouched until acceptance. This two-layer split is what makes the
mechanism reusable for contributions: an artist who submits a song via chat or the public web form
isn't a station owner and doesn't get station RLS granted directly on the fragment — instead the
fragment is created bare (visible only to the submitter's own account) and a PENDING share is created
pointing at the target station, with share-entity RLS granted to that station's owner so the offer is
reviewable. The station only gets fragment-level access once it **accepts**.

This also fixed a real bug: previously, a contributor who later registered a real `mixdeck` account
could never see their own submission again, because the fragment already had a brand association at
creation time — which excluded it from the one page (`/sound-library/unassigned-to-brands`) that
would otherwise show it to a plain registered user. Under this design the fragment has no brand
association until accepted, so it always shows up there for the submitter in the meantime.

Contributions used to be their own system entirely: a `LifecycleStatus`-driven `status` column
checked directly on the fragment, with a direct brand association/RLS grant at creation. That's gone
— `ApprovalStatus` (below) is the only status a contribution has, same as any other share.

---

## 1. Status lifecycle (`ApprovalStatus`, `model/cnst/ApprovalStatus.java`)

One enum, three values, used everywhere a share exists — regardless of whether it originated as a
station-to-station offer or an artist contribution:

| Value | Meaning |
|---|---|
| `506` `PENDING` | offered, awaiting the receiving station's decision |
| `500` `ACCEPTED` | accepted |
| `501` `REJECTED` | rejected by receiver |

(Numeric values reuse the old enum's existing operational codes for `PENDING`/`OPEN`/`CANCELLED` —
no data migration was needed. The old `505 ACCEPTED` duplicate and `502 REJECTED_NOT_MEET_GENRE`
values are gone.)

**Every new share starts `PENDING`, unconditionally.** Offering a share
(`SharedSoundFragmentService.patchShares` → `validateAndBuildEntities`) still checks the target
brand's `submissionPolicy == NO_RESTRICTIONS`, but there is **no automatic accept/reject based on
genre fit anymore** — genre is shown to the reviewing station owner as context (rendered as tags),
never used as an automated gate. A station can share (or a submitter can contribute) a fragment that
doesn't match the target's usual genres; the human reviewer decides.

---

## 2. Accept / reject

`SharedSoundFragmentRepository`:
- `acceptByReceiver(shareId, userId)` — sets `status = ACCEPTED`, inserts a
  `mixpla__brand_sound_fragments` row for the receiving brand (`ON CONFLICT DO NOTHING` — this is
  what makes the song show up in the receiving brand's regular library), **and** grants the
  receiving brand's owner + co-owners RLS on the underlying `mixpla__sound_fragments` row itself
  (`grantFragmentRlsToBrand`, raw-JSON `owner`/`coOwners` extraction mirroring
  `BrandRepository.getAllOpenForSubmission`). This RLS grant was a pre-existing gap — accepting used
  to update only the share status and the brand association, never the fragment's own RLS, so an
  accepted share's fragment was never actually visible via `findForBrandFlat`. Fixed as part of this
  redesign since contributions depend on it too.
- `rejectByReceiver(shareId, userId)` — sets `status = REJECTED`, deletes the receiver's RLS row for
  the share entity itself.

Both are gated by an RLS-reader subquery against the **share-entity** RLS table
(`mixpla__shared_sound_fragment_readers`, not the fragment's own) — the caller must actually be a
granted reader of that share. See §0 for the two-RLS-layer split; note the asymmetry: only the
brand **owner** gets a share-entity reader row at creation, but both owner **and co-owners** get
fragment RLS on accept.

If a contribution's fragment ends up shared to more than one station (e.g. the submitter also sent it
directly, or a station that received it re-shares it onward), each target station has its own
independent share/status — accepting or rejecting for one station has no effect on another's.

---

## 2a. ⚠️ Known gap: revoking after accept never undoes fragment RLS

None of the three "undo" paths below reverse the `grantFragmentRlsToBrand` RLS grant
(`mixpla__sound_fragment_readers` rows for the brand owner + co-owners) made on accept. There is no
`revokeFragmentRlsFromBrand` anywhere in the codebase, and the DDL has no `ON DELETE CASCADE` from
that table to the share or to `mixpla__brand_sound_fragments` — so once granted, that row is
permanent unless someone deletes it out-of-band.

| Path | What it cleans up | What it leaves behind |
|---|---|---|
| Admin/sender `DELETE /shared/:id` (`SharedSoundFragmentController.delete` → `SharedSoundFragmentRepository.archive`) | Sets `archived = 1` on the share row | `mixpla__brand_sound_fragments` row **and** fragment RLS both untouched — brand keeps the song and direct RLS indefinitely |
| Sender removes a target brand via `patchShares` (`SharedSoundFragmentRepository.deleteInTx`) | Hard-deletes the share row + share-entity RLS row | Same as above — brand association and fragment RLS survive, orphaned, with no share record left to explain why |
| Receiver `rejectByReceiver` | Deletes the `mixpla__brand_sound_fragments` row, updates `status = REJECTED`, deletes share-entity RLS row | Fragment RLS (owner **and** co-owners) is *not* revoked — they keep standing direct read access to the underlying `SoundFragment` even after rejection |

`rejectByReceiver` is the most complete of the three, but even it leaves a stale grant. This mirrors a
wider pattern in this repo — `SoundFragmentBrandAssociationHandler.removeBrands` (plain brand-library
unassignment, unrelated to sharing) also drops `mixpla__brand_sound_fragments` without touching
fragment RLS — so it isn't a sharing-specific oversight, but it does mean "revoke a share" is
currently a partial operation. Fixing it would mean adding a `revokeFragmentRlsFromBrand(tx,
soundFragmentId, targetBrandId)` (delete from `mixpla__sound_fragment_readers` for that brand's
owner/co-owners, mirroring `grantFragmentRlsToBrand`) and calling it from all three paths above —
not yet implemented.

---

## 3. Creating a share

Two entry points into `SharedSoundFragmentService`, both building a plain `SharedSoundFragment` and
funneling into the same `SharedSoundFragmentRepository.applyPatch`/`insertInTx`:

- `patchShares(...)` — station-to-station: an authenticated station owner offers one of their own
  fragments to one or more target brands (`validateAndBuildEntities`).
- `shareContribution(soundFragmentId, targetBrandId, submitterUserId, submitterName, submitterEmail)`
  — called from `SoundFragmentService.createFromBulkUpload` right after a public/chat contribution's
  fragment is inserted, when a target station was specified. See §4 for the contribution side.

Both produce a `PENDING` share; there is no other status a freshly-created share can have.

---

## 4. Contribution entry points

A **contribution** is a sound fragment submitted by an artist for a station owner to review — either
through the AI chatbot (`jesoos`) or the public, unauthenticated web form (`mixdeck` `/submission`).
Both entry points create a normal row in `mixpla__sound_fragments` tagged `source = CONTRIBUTION`,
then — if a target station was specified — a share (§3) pointing at that station.

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
3. `POST /public/songs/chunk` (used by the FE) — `email`/`code` are required and re-validated on
   **every** chunk request (`401` otherwise), to support multi-chunk uploads without a single-use
   code. `stationSlug` is only required (and only read) on the **first** chunk (`chunkIndex == 0`) —
   `FileUploadService.resolveBrandSlugIfNeeded` resolves it to a brand id once and caches it per
   `batchId`, so later chunks don't need to resend it. Descriptive metadata (`artistName`, `genre`,
   `country`, `description`) is likewise only read from the **last** chunk — it's the only one
   `assembleAndProcess` ever uses it from. Chunks stream to disk; on the last chunk,
   `FileUploadService.assembleAndProcess` extracts audio metadata (FFprobe via `AudioMetadataService`)
   and calls `SoundFragmentService.createFromBulkUpload(..., requiresApproval=true, meta)`.
4. Post-upload processing status (metadata extraction, entity creation) is polled via
   `GET /public/songs/status/:batchId/stream` (SSE) — the same `FileUploadService.streamBulkProgress`
   mechanism the authenticated bulk-upload dialog uses, reused as-is rather than duplicated, since both
   poll the same `bulkUploadProgressMap` keyed by `batchId`.

### `requiresApproval` — one method, two callers

`createFromBulkUpload` is shared with `SoundFragmentBulkUploadController` (the **authenticated**
station-owner bulk-upload feature — a different, unrelated flow: self-owned content, no review
needed). `FileUploadService` derives `requiresApproval` from which `controllerKey` called it
(`"public-submissions"` vs `"sound-fragments-controller"`), so:

- `requiresApproval = false` (authenticated bulk upload): unchanged original behavior — `USER_UPLOAD`
  source, direct brand association, no share involved.
- `requiresApproval = true` (public/chat submission): `CONTRIBUTION` source, **no** brand association
  at creation (`brandIds = List.of()`), RLS granted only to the submitter's resolved account (below)
  and `SuperUser` directly on the fragment — and, when a target station was specified, a `PENDING`
  share is created via `SharedSoundFragmentService.shareContribution(...)` right after insert.

**Never** change this method's behavior without checking both call sites — a blanket change here
would silently affect the authenticated station owner's own uploads.

Both paths set `fragment.setStatus(1)` — a fixed placeholder value, not `LifecycleStatus` and not
`ApprovalStatus`. The fragment itself no longer carries an approval status; the share does.

### Silent account resolution (no separate "claim" step)

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

## 5. Where status is (and isn't) enforced

| Query | Enforces status? |
|---|---|
| `SharedSoundFragmentRepository.getReceivedList`/`getReceivedListCount` (received queue) | No — shows every share regardless of `ApprovalStatus`, the FE renders status as a tag. |
| `SoundFragmentBrandRepository.findForBrandFlat`/`findForBrandCount` (station's regular library page) | Implicitly — a fragment only appears here once `mixpla__brand_sound_fragments` has a row for that brand, which only happens on share acceptance. No separate status check is needed (an earlier `CONTRIBUTION`-specific status gate here was removed as part of this redesign — brand association itself is now sufficient gating). |
| `jesoos`'s playback-agenda query | Out of scope for this repo. If it still special-cases `source = CONTRIBUTION` + a `LifecycleStatus` value, that check is obsolete under this design and should be reverted — a `CONTRIBUTION` fragment is eligible for playback exactly when it has a `mixpla__brand_sound_fragments` row, same as any other fragment. |

If you add a **new** query that lists or selects sound fragments for a brand, you do not need a
contribution-specific status gate — brand association (created only on share acceptance) is already
the correct scope.

---

## 6. The "received" inbox

`mixdeck`'s `/sound-library/received` (`PendingReviewView.vue`) lists shares for the current user via
the existing `/shared-sound-fragments/received*` routes — plain, single-source queries against
`SharedSoundFragmentRepository` (`getSharingPreviewList`/`getSharingPreviewCount`/`getById`). Because
contributions are now just shares, no merge/dispatch logic is needed here — a contribution and a
station-to-station offer are indistinguishable rows in this list, both driven by the one
`ApprovalStatus` enum. The FE shows a single status tag (`PENDING`/`ACCEPTED`/`REJECTED`); there is no
"origin" concept to display.

`SharingPreviewDTO` (`dto/sharing/SharingPreviewDTO.java`) carries `title`, `artist`, `genres`,
`labels`, `sharerUserName`/`sharerUserEmail`, `targetBrandName`, `boost`, `status` — no `origin` or
`regDate` fields; those were removed since there's no longer a multi-source merge/sort to support.

---

## Key files

| Area | File |
|---|---|
| Routes (`received`, `received/:id/accept`, `received/:id`) | `rest/SharedSoundFragmentController.java` |
| Share creation, accept/reject, received-list mapping | `service/soundfragment/SharedSoundFragmentService.java` |
| Share table queries, accept/reject transactions, fragment-RLS grant on accept | `repository/soundfragment/SharedSoundFragmentRepository.java` |
| Status enum | `model/cnst/ApprovalStatus.java` |
| Received-inbox DTO | `dto/sharing/SharingPreviewDTO.java` |
| Share entity model | `model/soundfragment/SharedSoundFragment.java` |
| Public submission entry point | `rest/PublicSongSubmissionController.java` |
| OTP verification | `service/OtpService.java` |
| Chunk assembly / dispatch | `service/util/FileUploadService.java` |
| Fragment creation, submitter resolution, share creation trigger | `service/soundfragment/SoundFragmentService.java` (`createFromBulkUpload`, `resolveSubmitterAccount`) |
| Submission metadata carrier | `dto/PublicSubmissionMetaDTO.java` |
| Chatbot entry point (other repo) | `jesoos` — `service/chat/tools/UploadSongToolHandler.java`, and that repo's `CHAT_WORKFLOW.md` |
