# Sharing Workflow — datanest

A **share** is one station (brand) offering a sound fragment it owns to another station, who can
accept or reject it. This lives entirely in `mixpla__shared_sound_fragments` / `SharedSoundFragment`,
tracked by `ApprovalStatus` — a completely separate table and enum from **contributions** (artist
submissions via chat or the public web form; see `CONTRIBUTION_WORKFLOW.md` in this same directory).
The two only meet at the FE "received" inbox, which merges both — see §3.

---

## 0. Why this exists (vs. contribution)

Sharing answers: **"do I also want a copy of this song, which another station already owns and
vetted, in my own library?"** Both sides here are already trusted station owners — nothing new is
entering the platform, an existing, already-legitimate song is just being copied between libraries.

Contribution (see `CONTRIBUTION_WORKFLOW.md`) answers a different question: **"should this brand-new
song be allowed onto the platform at all?"**, decided by one station owner about a submitter who isn't
already part of the system. That's a gatekeeping decision about new content, not a distribution
decision about existing content — hence the different table/enum/rules. The two only look similar
because both feel like "something is waiting for my approval" from a station owner's point of view,
which is the only reason they're merged into one FE "received" inbox.

| | Contribution | Sharing |
|---|---|---|
| Who hands over the song | An outside artist (often no account yet) | A station owner, to another station owner |
| Decision being made | "Is this new song okay to exist here at all?" | "Do I want a copy of this in my own library?" |
| Tracked on | the song itself (`sf.status`) | the *relationship* between the two stations |
| If rejected | the song stays dead in the DB — only that submitter is affected | only *this* station's copy is cancelled — the original owner's copy is untouched |

---

## 1. Status lifecycle (`ApprovalStatus`, `model/cnst/ApprovalStatus.java`, deprecated but still in use)

| Value | Meaning |
|---|---|
| `506` `PENDING` | offered, awaiting the receiving brand's decision |
| `500` `OPEN` | accepted |
| `505` `ACCEPTED` | accepted (legacy duplicate meaning, see `ShareDTO.setShared`) |
| `501` `CANCELLED` | rejected by receiver |
| `502` `REJECTED_NOT_MEET_GENRE` | rejected — receiving brand's genre restrictions didn't match at offer time |
| `503` `REJECTED` | rejected |

Offering a share (`SharedSoundFragmentService.patchShares` → `validateAndBuildEntities`) checks the
target brand's `submissionPolicy == NO_RESTRICTIONS` and whether genres match, but does **not** check
the source fragment's own approval status — a station can share a fragment of theirs that is itself
still an unreviewed contribution (see `CONTRIBUTION_WORKFLOW.md` §3 for what that implies once shared).

---

## 2. Accept / reject

`SharedSoundFragmentRepository`:
- `acceptByReceiver(shareId, userId)` — sets `status = OPEN`, and inserts a
  `mixpla__brand_sound_fragments` row for the receiving brand (`ON CONFLICT DO NOTHING`) — this is what
  actually makes the song show up in the receiving brand's regular library.
- `rejectByReceiver(shareId, userId)` — sets `status = CANCELLED`, deletes the receiver's RLS row for
  the share entity (unlike rejecting a contribution, which leaves RLS untouched — see
  `CONTRIBUTION_WORKFLOW.md` §3).

Both are gated by an RLS-reader subquery — the caller must actually be a granted reader of that share.

---

## 3. The unified "received" inbox — no new endpoint

`mixdeck`'s `/sound-library/received` shows **both** shares and contributions in one list. Rather than
a new endpoint or a client-side merge of two API calls, the *existing* `/shared-sound-fragments/received*`
routes were extended to internally dispatch to whichever system owns a given row — reuse the endpoint
that already represented "my received inbox" instead of growing new API surface for the same concept.

`SharedSoundFragmentService`:
- `getSharingPreviewList`/`getSharingPreviewCount` — fetch `offset+limit` rows from **both**
  `SharedSoundFragmentRepository.getReceivedList` (shares) and
  `SoundFragmentRepository.getPendingApprovalList` (contributions), tag each with `origin`
  (`"SHARE"`/`"SUBMISSION"`), merge, sort by `regDate` desc, then slice. Fetching `offset+limit` from
  each side (not just `limit`) is what guarantees the slice is correct regardless of how the two
  sources interleave by date — the alternative, a SQL `UNION ALL` across two very differently-shaped
  queries, was skipped since this is a review queue (low volume); revisit only if that stops being true.
- `acceptShareByReceiver`/`rejectShareByReceiver`/`getById` — try the share-repository method first; if
  it affects `0` rows (not a share), fall back to the contribution-repository equivalent
  (`approvePendingFragment`/`rejectPendingFragment`/`findPendingApprovalById`). The frontend always
  calls the same `PATCH .../accept` / `DELETE ...` / `GET .../:id` regardless of a row's origin — it
  never needs to know or send which system owns a given id; the id itself determines that.

Both origins map into the **same** `SharingPreviewDTO` (`dto/sharing/SharingPreviewDTO.java`) rather
than a parallel DTO type — its existing fields (`title`, `artist`, `genres`, `labels`,
`sharerUserName`/`sharerUserEmail`, `boost`, `status`) already cover a contribution row; only `origin`
and `regDate` needed adding.

---

## Key files

| Area | File |
|---|---|
| Routes (`received`, `received/:id/accept`, `received/:id`) | `rest/SharedSoundFragmentController.java` |
| Merge / dispatch logic | `service/soundfragment/SharedSoundFragmentService.java` |
| Share table queries | `repository/soundfragment/SharedSoundFragmentRepository.java` |
| Status enum | `model/cnst/ApprovalStatus.java` |
| Unified DTO (shares + contributions) | `dto/sharing/SharingPreviewDTO.java` |
| Share entity model | `model/soundfragment/SharedSoundFragment.java` |
