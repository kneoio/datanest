# Brand-team song visibility — datanest

**Policy:** every **owner + co-owner** of a brand sees **all** songs assigned to that brand, no
matter which team member saved them. A user's visible library is the **union of (owner + co-owners)
across every brand a fragment is assigned to** — role (owner vs co-owner) does not matter, brand
membership does.

This layers on top of the base RLS mechanism (`RLS_WORKFLOW.md`) and reuses the same fragment-RLS
grant as station-to-station accept (`SHARING_WORKFLOW.md` §0/§2).

---

## Why the plain RLS grant isn't enough

The brand-library read is scoped by **per-user** fragment RLS, not by brand membership:

```sql
-- SoundFragmentBrandRepository.findForBrandFlat
JOIN mixpla__sound_fragment_readers rls ON t.id = rls.entity_id
WHERE bsf.brand_id = $1 AND rls.reader = $2   -- $2 = the caller
```

On save, the creator gets a fragment reader row, but co-owners of the same brand do **not** — so
without an extra grant each team member only sees their own songs even inside a shared brand.

---

## The two grant points

Both grant a **full** (`can_edit = true`, `can_delete = true`) fragment reader row to the brand's
owner and every co-owner. Both are `ON CONFLICT DO NOTHING` (idempotent) and run inside the caller's
transaction. SQL mirrors `SharedSoundFragmentRepository.grantFragmentRlsToBrand`.

| When | Where | Scope |
|---|---|---|
| A fragment is assigned to a brand (SF create **or** update — both funnel through `addBrands`) | `SoundFragmentBrandAssociationHandler.grantFragmentRlsToBrands` | that fragment × each newly-added brand's owner + co-owners |
| A brand is saved (`update`) | `BrandRepository.backfillFragmentRlsForBrandMembers` | every song already assigned to the brand × the brand's current owner + co-owners |

The **backfill on brand-save** is what makes a **newly added co-owner** retroactively see songs saved
before they joined — the grant reads `b.owner` after the owner JSON is written in the same UPDATE, so
it always reflects the new membership.

---

## Deliberate non-goals

- **No revoke.** Removing a user from owner/co-owner does **not** drop their fragment reader rows —
  once granted, access stays. There is no `revoke` path (same known gap as `SHARING_WORKFLOW.md` §2a).
  New members obviously only gain songs that exist at add-time or on the next save.
- **No schema change.** Reuses `mixpla__sound_fragment_readers` as-is.
- **Full access, not read-only.** Co-owners can edit and delete each other's songs by design.
