# RLS Workflow — datanest

Row-Level Security (RLS) is how datanest scopes every row to the user allowed to see it. datanest is
the CRUD backend for the **Mixdeck** (subscriber) and **42next** (admin) SPAs, so **every** query is
user-scoped — a user only ever sees/edits rows they have been granted.

> `jesoos` and `aivox` do **not** use RLS. They run as a trusted system user (`SuperUser`) and skip
> the ACL join for performance. RLS is a **datanest-only** concern — never port it into the backend
> services.

---

## Mechanism

Every entity table `X` has a companion **ACL / RLS table** (`entityData.getRlsName()`, e.g.
`X_readers`) with:

| Column | Meaning |
|---|---|
| `reader` | user id allowed to access the row |
| `entity_id` | FK to `X.id` |
| `can_edit` | may update the row |
| `can_delete` | may delete the row |
| `reading_time` | last read bookkeeping |

Implemented in the shared core (`2next`): `com.semantyca.core.repository.AsyncRepository`,
`repository.rls.RLSRepository`, `repository.rls.RlsActionUtil`. datanest repositories extend
`AsyncRepository` and pass the `IUser` into every call.

### Read — scoped
Every select joins the entity with its ACL table on the caller:

```sql
SELECT t.* FROM X t JOIN X_readers rls ON t.id = rls.entity_id
WHERE rls.reader = <user.id>
```

No matching ACL row ⇒ the record is invisible to that user. `count`, `getById`, and list/paged
queries are scoped the same way — there is no unscoped read path.

### Create — grant on insert
On insert the repository, in one transaction:
1. inserts the entity, then
2. `insertRLSPermissions` — grants the **creator** a reader row (with `can_edit` / `can_delete`), then
3. `applyRlsActions(RlsActionDTO…)` — applies any additional shares to other readers.

### Update / Delete — gated
The caller's ACL row must carry `can_edit` (update) or `can_delete` (delete); without the flag the
write is rejected.

### Sharing
Access is granted to other users by adding reader rows (via `RlsActionDTO` / `RlsActionUtil`),
optionally with edit/delete rights — this is how a resource is shared between subscribers/admins.

**Delayed grant on share/accept.** Sound fragment sharing (station-to-station, and artist
contributions — see `soundfragment/SHARING_WORKFLOW.md`) is a variation
on grant-on-insert: the target station gets **no** RLS row on the fragment at share-creation time,
only on the receiver's own share entity. The fragment-level grant only happens when the target
station **accepts** the share (`SharedSoundFragmentRepository.acceptByReceiver` →
`grantFragmentRlsToBrand`). Follow this pattern (grant on acceptance, not on offer) for any similar
"offer, then the other side opts in" feature.

One deliberate, narrow exception: a receiver can preview a still-PENDING share's audio before
deciding, via a query that bypasses fragment RLS entirely (safe only because it's gated by the
share-entity RLS check that already ran first) — see `soundfragment/SHARING_WORKFLOW.md` §2b. That
section also flags an adjacent, unrelated, **not yet fixed** gap: the actual file-serving endpoint
doesn't appear to enforce fragment RLS at all, for anyone, independent of the preview feature.

---

## Rules

- **Always pass the real `IUser`** into datanest repositories; never bypass the ACL join or read
  unscoped.
- **New CRUD entity ⇒ wire RLS from day one:** ACL table + scoped queries + grant-on-insert, exactly
  like the existing repositories (`AiAgentRepository`, `SceneRepository`, …).
- **Repository only via its Service** (see datanest CLAUDE.md conventions) — the RLS-scoped access
  stays behind the service boundary.
- Do **not** copy this into `jesoos` / `aivox` — they are intentionally RLS-free for speed.
