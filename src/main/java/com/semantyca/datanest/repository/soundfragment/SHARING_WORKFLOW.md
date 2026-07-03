# Sharing Workflow — datanest

A **share** is one entity (a station, or an outside artist submitting a song) offering a sound
fragment to a target station, who can accept or reject it. Everything lives in
`mixpla__shared_sound_fragments` / `SharedSoundFragment`, tracked by one `ApprovalStatus` enum.

**Station-to-station sharing and artist contributions are now the same mechanism.** There used to
be two separate systems (a `SharedSoundFragment`/`ApprovalStatus` table for shares, and a
`LifecycleStatus`-driven flag directly on the fragment for contributions) — see
`CONTRIBUTION_WORKFLOW.md` in this same directory for why that was replaced and what it now looks
like from the contribution side. This doc covers the mechanism itself.

---

## 0. Why this exists

The fragment being offered gets **no brand association and no direct RLS to the target station at
creation time**. Visibility for the target station comes entirely from the share row. This is what
makes the mechanism reusable for contributions: an artist who submits a song via chat or the public
web form isn't a station owner and doesn't get station RLS granted directly — instead the fragment
is created bare (visible only to the submitter's own account) and a PENDING share is created
pointing at the target station. The station only gets access once it **accepts**.

This also fixed a real bug: previously, a contributor who later registered a real `mixdeck` account
could never see their own submission again, because the fragment already had a brand association at
creation time — which excluded it from the one page (`/sound-library/unassigned-to-brands`) that
would otherwise show it to a plain registered user. Under this design the fragment has no brand
association until accepted, so it always shows up there for the submitter in the meantime.

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

Both are gated by an RLS-reader subquery — the caller must actually be a granted reader of that
share.

---

## 3. Creating a share

Two entry points into `SharedSoundFragmentService`, both building a plain `SharedSoundFragment` and
funneling into the same `SharedSoundFragmentRepository.applyPatch`/`insertInTx`:

- `patchShares(...)` — station-to-station: an authenticated station owner offers one of their own
  fragments to one or more target brands (`validateAndBuildEntities`).
- `shareContribution(soundFragmentId, targetBrandId, submitterUserId, submitterName, submitterEmail)`
  — called from `SoundFragmentService.createFromBulkUpload` right after a public/chat contribution's
  fragment is inserted, when a target station was specified. See `CONTRIBUTION_WORKFLOW.md` for the
  caller side.

Both produce a `PENDING` share; there is no other status a freshly-created share can have.

---

## 4. The "received" inbox

`mixdeck`'s `/sound-library/received` (`PendingReviewView.vue`) lists shares for the current user via
the existing `/shared-sound-fragments/received*` routes — plain, single-source queries against
`SharedSoundFragmentRepository` (`getSharingPreviewList`/`getSharingPreviewCount`/`getById`). Because
contributions are now just shares, no merge/dispatch logic is needed here — a contribution and a
station-to-station offer are indistinguishable rows in this list, both driven by the one
`ApprovalStatus` enum. The FE shows a single status tag (`PENDING`/`ACCEPTED`/`REJECTED`); there is no
more "origin" concept to display.

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
