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
| Share-entity RLS | `mixpla__shared_sound_fragment_readers` (entity_id = the `SharedSoundFragment` row) | target brand's owner **+ co-owners** + `SuperUser` (`insertRlsForReceivers`) | immediately at share creation |
| Fragment RLS | `mixpla__sound_fragment_readers` (entity_id = the `SoundFragment` row) | target brand's owner **+ co-owners** (`grantFragmentRlsToBrand`) | only on **accept** |

The share-entity RLS is what makes the offer show up in the target station owner's `/received` inbox
at all, and is what `acceptByReceiver`/`rejectByReceiver`/`archiveByReceiver` check (`... id IN
(SELECT entity_id FROM mixpla__shared_sound_fragment_readers WHERE reader = $2)`) before allowing a
decision — a station owner who was never granted a reader row on that specific share cannot act on
it. It says nothing about whether the underlying song itself is visible anywhere else (brand library,
playback, etc.) — that's the fragment-RLS layer, untouched until acceptance. This two-layer split is
what makes the mechanism reusable for contributions: an artist who submits a song via chat or the
public web form isn't a station owner and doesn't get station RLS granted directly on the fragment —
instead the fragment is created bare (visible only to the submitter's own account) and a PENDING share
is created pointing at the target station, with share-entity RLS granted to that station's owner and
co-owners so the offer is reviewable by any of them. The station only gets fragment-level access once
it **accepts**.

⚠️ **One deliberate, narrow exception to "no fragment access before accept":** the receiver can
preview the audio of a still-PENDING share before deciding. See §2b.

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

**Re-sharing the same fragment to the same brand always resets it to `PENDING`.** `insertInTx`'s
upsert (`ON CONFLICT ON CONSTRAINT unique_brand_shared_fragment DO UPDATE SET archived = 0, status =
EXCLUDED.status, ...`) means there is only ever one row per (fragment, target brand) pair — sharing
"twice" to the same brand never creates a duplicate, it silently revives/resets the existing row
(un-archives it, resets status to `PENDING`), no error or warning either way. Sharing to *different*
brands simultaneously is unrelated and fully independent — each target brand gets its own row/status.

---

## 2. Accept / reject / delete (receiver side)

`SharedSoundFragmentRepository`:
- `acceptByReceiver(shareId, userId)` — sets `status = ACCEPTED`, inserts a
  `mixpla__brand_sound_fragments` row for the receiving brand (`ON CONFLICT DO NOTHING` — this is
  what makes the song show up in the receiving brand's regular library), **and** grants the
  receiving brand's owner + co-owners RLS on the underlying `mixpla__sound_fragments` row itself
  (`grantFragmentRlsToBrand`, raw-JSON `owner`/`coOwners` extraction mirroring
  `BrandRepository.getAllOpenForSubmission`).
- `rejectByReceiver(shareId, userId)` — sets `status = REJECTED` and drops the
  `mixpla__brand_sound_fragments` row (harmless no-op if accept never happened). **It no longer
  touches RLS at all.** It used to also delete every share-entity reader row the instant the share
  was rejected, which made `status = REJECTED` unobservable — the record vanished for everyone
  (including `SuperUser`) before anyone could see it was rejected, and there was no way back to
  remove it deliberately later. Now the share just sits there, still visible to the receiver (and
  the sharer, see §2c), tagged `REJECTED`, until someone explicitly removes it.
- `archiveByReceiver(shareId, userId)` — the actual removal step, **gated on `status = REJECTED`**
  (`WHERE id = $1 AND status = 501 AND id IN (SELECT entity_id FROM ... WHERE reader = $2)`). Sets
  `archived = 1` on the share row. This is what the receiver's UI "Delete" action calls once an item
  is already rejected — it cannot be used to silently drop access to a still-PENDING or
  already-ACCEPTED share.

**Accept and reject are freely reversible in either direction, any number of times** — neither has a
`status` guard, so REJECTED → accept and ACCEPTED → reject both work (undoing a rejection re-inserts
the `mixpla__brand_sound_fragments` row and re-grants fragment RLS; un-accepting removes the library
row but does **not** revoke the fragment RLS granted on the original accept — see §2a). Both **do**
check `archived = 0` — once a receiver has deleted a share via `archiveByReceiver`, it can no longer
be accepted or rejected back to life through these two methods (they'd match zero rows, same as a
nonexistent id or an unauthorized caller). Re-sharing (§3) is the only way to bring an archived share
back, and that resets it to a fresh `PENDING` row, not whatever it was before archiving.

Routes (`rest/SharedSoundFragmentController.java`):

| Route | Handler | Does |
|---|---|---|
| `PATCH /received/:id/reject` | `rejectShareByReceiver` | sets `status = REJECTED`, keeps everything visible |
| `DELETE /received/:id` | `archiveRejectedByReceiver` | archives (only if already `REJECTED`) |
| `PATCH /received/:id/accept` | `acceptShareByReceiver` | unchanged |

Both accept and reject are gated by an RLS-reader subquery against the **share-entity** RLS table
(`mixpla__shared_sound_fragment_readers`, not the fragment's own) — the caller must actually be a
granted reader of that share. See §0 for the two-RLS-layer split — both layers grant owner **and**
co-owners (`insertRlsForReceivers` mirrors `grantFragmentRlsToBrand`'s owner+coOwners pattern; it used
to extract only the single scalar `owner->>'userId'` via `RlsActionUtil.grantFromJsonField`, silently
never granting co-owners a share-entity reader row at all — fixed).

If a contribution's fragment ends up shared to more than one station (e.g. the submitter also sent it
directly, or a station that received it re-shares it onward), each target station has its own
independent share/status — accepting or rejecting for one station has no effect on another's.

---

## 2a. ⚠️ Known gap: revoking after accept never undoes fragment RLS

None of the "undo" paths below reverse the `grantFragmentRlsToBrand` RLS grant
(`mixpla__sound_fragment_readers` rows for the brand owner + co-owners) made on **accept**. There is
no `revokeFragmentRlsFromBrand` anywhere in the codebase, and the DDL has no `ON DELETE CASCADE` from
that table to the share or to `mixpla__brand_sound_fragments` — so once granted, that row is
permanent unless someone deletes it out-of-band. This only applies to shares that were actually
**accepted** at some point — a share that's only ever been PENDING/REJECTED never had fragment RLS
granted in the first place, so there's nothing to leak there (see §2 — `rejectByReceiver` correctly
has nothing to revoke on that layer).

| Path | What it cleans up | What it leaves behind |
|---|---|---|
| Admin/sender `DELETE /shared/:id` (`SharedSoundFragmentController.delete` → `SharedSoundFragmentRepository.archive`) | Sets `archived = 1` on the share row | `mixpla__brand_sound_fragments` row **and** fragment RLS both untouched — brand keeps the song and direct RLS indefinitely |
| Sender removes a target brand via `patchShares` (`SharedSoundFragmentRepository.deleteInTx`) | Hard-deletes the share row + share-entity RLS row | Same as above — brand association and fragment RLS survive, orphaned, with no share record left to explain why |
| Receiver `archiveByReceiver` | Sets `archived = 1` on the share row (only reachable once already `REJECTED`, so `mixpla__brand_sound_fragments` and fragment RLS were never granted for this share in the first place — nothing to leak here) | n/a |

Fixing the ACCEPTED-then-revoked case would mean adding a `revokeFragmentRlsFromBrand(tx,
soundFragmentId, targetBrandId)` (delete from `mixpla__sound_fragment_readers` for that brand's
owner/co-owners, mirroring `grantFragmentRlsToBrand`) and calling it from the two accepted-share paths
above — not yet implemented.

---

## 2b. Receiver can preview audio before deciding (deliberate RLS exception)

A receiver needs to actually *listen* to a pending share to assess it before accepting/rejecting —
but per §0, fragment RLS isn't granted until accept. `SharedSoundFragmentRepository.findById(id,
userId)` (the single-item fetch behind `GET /received/:id`) chains `attachPreviewFiles`, which queries
`_files` directly by `sound_fragment_id`, **bypassing the fragment's own RLS table entirely**. This is
safe *only* because `attachPreviewFiles` is reached exclusively after `findById`'s own share-entity
RLS check already passed — the caller is already a confirmed authorized reader of this specific share,
just not yet of the fragment itself. Exposed as `SharingPreviewDTO.uploadedFiles` (`UploadFileDTO`
list, same shape as the normal fragment DTO's files, `type` = `"opus"`/`"original"`), populated **only**
on the single-item fetch (`getById`), never on the paged `getReceivedList`/`getSharingPreviewList` (no
N+1 file queries on the list view).

The mixdeck FE (`ReceivedForm.vue`) renders this with the same `AudioMiniPlayer` component and
opus-preferred file selection logic as `SoundFragmentForm.vue`.

⚠️ **Related, unfixed gap (flagged separately, not yet addressed):** the file-serving endpoint this
preview relies on, `GET /soundfragments/files/:id/:slug` (`SoundFragmentController.getBySlugName` →
`FileDownloadService.getFile` → ... → `SoundFragmentRepository.getFileBySlugName(id, slugName, user,
false)` → `fileHandler.getFileBySlugName(id, slugName)`), does not appear to check fragment RLS at
all for cloud-stored files — `user` is passed in but never used for an ownership check. That means,
independent of the receiver-preview feature above, **any authenticated user who knows a fragment's
UUID and file slug name can currently fetch its audio regardless of RLS.** Any fix here must not
break the preview flow above, which relies on this endpoint being reachable for a receiver who only
has share-entity RLS, not fragment RLS.

---

## 2c. Sender's view of a share's status (`sharedWith`)

`SharedSoundFragmentRepository.listBySoundFragmentId` (backing `SoundFragmentService.getDTO`'s
`sharedWith` field, shown in `SoundFragmentForm.vue`'s "Sharing" tab) filters only on
`ssf.archived = 0` — **no status filter**. So a rejected share is still returned to the sender with
`status = 501`; nothing on the backend hides it. `ShareDTO.shared` (`status != REJECTED`) is a legacy
convenience flag the mixdeck FE used to filter rejected entries out of view entirely — it no longer
does that (shows every entry with a status tag instead, rejected rendered as a muted "Not accepted"
rather than an alarming red "Rejected", to avoid it reading as something having gone wrong). The
`shared` field itself is still sent but is effectively unused by the current FE — don't assume it
still drives visibility if you're reading old code/docs that reference it.

⚠️ **This "always visible to the sender" guarantee only holds until the receiver deletes it.**
`archiveByReceiver` (§2) sets `archived = 1` on the *same* row `listBySoundFragmentId` filters on —
so once a receiver permanently deletes their rejected copy, it silently disappears from the sender's
`sharedWith` list too, with no trace and no notice to the sender. This was a known, discussed
trade-off (not an oversight) — the alternative would require decoupling "receiver's visibility" from
"sender's visibility" onto separate flags, which hasn't been built. If a future session is asked to
make the sender's history durable regardless of what the receiver does, this is the place to start.

---

## 3. Creating a share

Two entry points into `SharedSoundFragmentService`, both building a plain `SharedSoundFragment` and
funneling into the same `SharedSoundFragmentRepository.applyPatch`/`insertInTx`:

- `patchShares(fragmentId, slug, patch, user)` — station-to-station: an authenticated station owner
  offers one of their own fragments to one or more target brands (`validateAndBuildEntities`). `slug`
  identifies **which of the sharer's stations** is doing the sharing — not for permission (fragment
  access is already RLS-checked separately via `soundFragmentRepository.findById(fragmentId,
  user.getId(), ...)`), but to resolve *whose identity to attribute the share to*: that station's
  owner's name/email becomes `sourceUserName`/`sourceUserEmail`, shown to the receiver as "shared by".
  A fragment can be assigned to more than one of a user's brands, so slug picks which one's identity
  is credited.
  - **`NO_BRAND_SLUG = "NO_BRAND"` sentinel:** a fragment with no brand association at all (e.g. from
    mixdeck's "unassigned to brands" page, `ArchivedView.vue`) has no station to pick a slug for. The
    FE sends this literal string as `slug` instead; `patchShares` detects it and attributes the share
    to `user` directly (`user.getId()/getUserName()/getEmail()`) rather than resolving a source brand
    — same attribution model as a contribution's `sourceUserId`. This is a single method with an
    `if (NO_BRAND_SLUG.equals(slug))` branch, not a separate endpoint — there is deliberately only one
    route (`PATCH /shared/:slug/:fragmentId`) for both cases.
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
| `SharedSoundFragmentRepository.listBySoundFragmentId` (sender's `sharedWith`) | No — same as above, see §2c. Only `archived` is filtered. |
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
`labels`, `sharerUserName`/`sharerUserEmail`, `targetBrandName`, `boost`, `status`, and (single-item
fetch only, see §2b) `uploadedFiles` — no `origin` or `regDate` fields; those were removed since
there's no longer a multi-source merge/sort to support.

---

## 7. SoundFragment delete/archive cascades to its shares

Deleting or archiving a `SoundFragment` used to never touch `mixpla__shared_sound_fragments` at all,
which had two consequences:

- **Hard delete** (`SoundFragmentService.delete` → `SoundFragmentRepository.deleteDatabaseRecords`):
  left orphaned share rows (and their share-entity RLS rows) pointing at a `sound_fragment_id` that no
  longer existed, forever.
  Fixed: `delete()` now calls `sharedSoundFragmentService.deleteBySoundFragmentId(uuid)` first (hard
  delete: RLS rows, then share rows), then deletes the fragment.
- **Soft delete / archive** (`SoundFragmentController`'s `DELETE /:id` actually calls `service.archive`,
  not `service.delete` — the REST "delete" for a sound fragment has always been a soft archive): left
  the share fully active — `listBySoundFragmentId` (§2c) kept showing it to the sender as if the
  fragment still existed, even though the receiver's queries (which join the fragment and check
  `sf.archived = 0`) had already stopped showing it. Inconsistent between the two sides.
  Fixed: `archive()` now also calls `sharedSoundFragmentService.archiveBySoundFragmentId(uuid)` (only
  if the main archive succeeded), setting `archived = 1` on every share for that fragment.

New repository methods: `SharedSoundFragmentRepository.deleteBySoundFragmentId(soundFragmentId)` /
`archiveBySoundFragmentId(soundFragmentId)`, both scoped by `sound_fragment_id` rather than share id.

---

## Key files

| Area | File |
|---|---|
| Routes (`received`, `received/:id/accept`, `received/:id/reject`, `received/:id`) | `rest/SharedSoundFragmentController.java` |
| Share creation (incl. `NO_BRAND_SLUG`), accept/reject/archive, received-list mapping | `service/soundfragment/SharedSoundFragmentService.java` |
| Share table queries, accept/reject/archive transactions, fragment-RLS grant on accept, audio-preview file fetch, SF-delete/archive cascade | `repository/soundfragment/SharedSoundFragmentRepository.java` |
| Status enum | `model/cnst/ApprovalStatus.java` |
| Received-inbox DTO (incl. `uploadedFiles`) | `dto/sharing/SharingPreviewDTO.java` |
| Share entity model (incl. `fileMetadataList`) | `model/soundfragment/SharedSoundFragment.java` |
| SoundFragment delete/archive → cascades into the above | `service/soundfragment/SoundFragmentService.java` (`delete`, `archive`) |
| Public submission entry point | `rest/PublicSongSubmissionController.java` |
| OTP verification | `service/OtpService.java` |
| Chunk assembly / dispatch | `service/util/FileUploadService.java` |
| Fragment creation, submitter resolution, share creation trigger | `service/soundfragment/SoundFragmentService.java` (`createFromBulkUpload`, `resolveSubmitterAccount`) |
| Submission metadata carrier | `dto/PublicSubmissionMetaDTO.java` |
| Chatbot entry point (other repo) | `jesoos` — `service/chat/tools/UploadSongToolHandler.java`, and that repo's `CHAT_WORKFLOW.md` |
| mixdeck: received-share detail view, incl. audio preview and reject/delete buttons | `mixdeck` — `src/components/forms/ReceivedForm.vue` |
| mixdeck: received-share list view, incl. reject-vs-delete bulk action | `mixdeck` — `src/views/ReceivedView.vue` |
| mixdeck: sender's `sharedWith` display (status tags) | `mixdeck` — `src/components/forms/SoundFragmentForm.vue` |
| mixdeck: share-to-brands dialog (incl. `NO_BRAND_SLUG` usage) | `mixdeck` — `src/components/forms/ShareToBrandsDialog.vue`, `src/services/datanestApi.ts` |
