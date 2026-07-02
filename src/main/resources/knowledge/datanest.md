---
id: datanest
title: datanest Service — Architecture & Conventions
summary: datanest CRUD backend — scope, conventions, RLS, repository/service/DTO pattern
tags: [datanest, crud, architecture, rls, conventions, quarkus]
---

# datanest

CRUD backend service for the Mixpla platform.

Works with:
- **Mixdeck** — user SPA
- **42next** — admin SPA

Stack: Quarkus 3.x, reactive (Vert.x / Mutiny), PostgreSQL, OIDC (Keycloak).

Port: 38799

## CRUD Pattern

Every entity has:
- **Repository** — DB access only
- **Service** — business logic + orchestration
- **Model** — internal entity class
- **DTO** — what crosses the REST/FE boundary; never expose the model directly

Repositories are private to their service. Never call another feature's repository directly.

## Row-Level Security (RLS)

RLS is always on in datanest — every query is user-scoped (backs the Mixdeck subscriber FE).

Detail: `src/main/java/com/semantyca/datanest/repository/RLS_WORKFLOW.md`

Do not bypass RLS. (jesoos/aivox skip it for performance — datanest does not.)

## API Pattern

- All routes registered via `setupRoutes(router)` in `DatanestApplication.java` — Vert.x reactive, not JAX-RS.
- Auth: role check per route via `requireRoles()` in `AbstractSecuredController`.
- Filter param: JSON string — `filter={"search":"..."}` (not individual query params).
- Pagination: `limit` + `offset` query params.
- Sort: `last_mod_date DESC` by default.

## Database

PostgreSQL, port 8572. DDL in `mxpldb` repo:
- `Scripts/mixpla_ddl.sql`
- `Scripts/2next_ddl.sql`

Schema changes require explicit user approval — propose `ALTER`s, do not apply.

## Shared Library (2next)

`com.semantyca:2next` — core models, DTOs, queue message shapes, utilities.

Change policy: user approval mandatory before any 2next change; bump versions in `pom.xml` and `EnvConst` before push.

## Observability

- Logging: `org.jboss.logging.Logger` with `warnf`/`errorf` and `%s` format (NOT SLF4J `{}` style).
- Metrics: publish important events as `MetricEventDTO` over RabbitMQ → metriq.

## Reactive Rules

- Use Mutiny `Uni`/`Multi`. Never block the event loop.
- Use `client.withTransaction(tx -> ...)` for multi-step DB writes.
- Use `RETURNING id` on INSERT and UPDATE to get the affected row id.

## Service Boundaries

datanest may be modified freely. Do not modify: `aivox`, `jesoos`, `metriq`, `nivaro`, `2next`.
For those services, only describe required changes.
