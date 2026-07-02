---
id: mixpla
title: Mixpla Platform — Infrastructure & Database Schema
summary: All services, ports, messaging, auth, shared library, and PostgreSQL schema
tags: [infrastructure, services, ports, architecture, database, schema, postgresql]
---

# Mixpla Platform

## Services

| Service  | Role                                                      | Stack                      | Port  |
|----------|-----------------------------------------------------------|----------------------------|-------|
| datanest | CRUD backend — brands, scripts, scenes, listeners, users  | Quarkus 3.x / Vert.x / PG | 38799 |
| jesoos   | Content delivery — scripts/scenes agenda, RabbitMQ sender | Quarkus 3.x / Vert.x       |       |
| aivox    | Audio mixer — HLS streaming, consumes jesoos messages     | Quarkus 3.x / Vert.x       |       |
| metriq   | Metric collector — dashboards, stats from all services    | Quarkus 3.x / Vert.x       |       |
| nivaro   | Finance — billing, subscriptions                          | Quarkus 3.x / Vert.x       |       |

## Front-ends

| App     | Role                          |
|---------|-------------------------------|
| Mixdeck | User SPA — talks to datanest  |
| 42next  | Admin SPA — talks to datanest |

## Messaging

- RabbitMQ for async communication: jesoos → aivox, and service metrics → metriq.

## Auth

- Keycloak OIDC at `https://auth.semantyca.com/realms/mixpla`.
- All datanest endpoints require a valid JWT with appropriate realm roles.
- Role checked per route via `requireRoles()` in AbstractSecuredController.

## Shared Library

- `2next` — core library (`com.semantyca.core.*`, `com.semantyca.mixpla.*`, `com.semantyca.officeframe.*`).
- Consumed by all services as a Maven package from GitHub Packages.

---

# Database Schema

- Engine: PostgreSQL
- Internal port: 8572

## Core Tables (2next — shared)

### `_users`
| Column       | Type               | Notes                     |
|--------------|--------------------|---------------------------|
| id           | BIGINT IDENTITY PK |                           |
| login        | VARCHAR(255) UNIQUE|                           |
| email        | CITEXT UNIQUE      |                           |
| search_name  | TEXT               | GIN-indexed for full-text |
| status       | INT                | UserRegStatus ordinal     |
| i_su         | BOOL               | superuser flag            |
| default_lang | INT                |                           |
| time_zone    | TEXT               |                           |

### `_user_labels`
| Column   | Type      | Notes                          |
|----------|-----------|--------------------------------|
| user_id  | BIGINT FK | → _users(id) ON DELETE CASCADE |
| label_id | UUID      |                                |

## Mixpla Tables

### `mixpla__brands`
| Column           | Type    | Notes                              |
|------------------|---------|------------------------------------|
| id               | UUID PK |                                    |
| slug_name        | VARCHAR | unique, URL-safe identifier        |
| search_name      | TEXT    | GIN trgm-indexed, auto via trigger |
| loc_name         | JSONB   | localized names                    |
| managing_mode    | ENUM    | SELF / MANAGED                     |
| script_mode      | VARCHAR | PREDEFINED / CUSTOM                |
| custom_script_id | UUID FK | → mixpla__scripts(id)              |
| ai_agent_id      | UUID FK | → mixpla__ai_agents(id)            |
| profile_id       | UUID FK | → mixpla__profiles(id)             |

### `mixpla__scripts`
| Column   | Type    | Notes                |
|----------|---------|----------------------|
| id       | UUID PK |                      |
| title    | VARCHAR |                      |
| loc_name | JSONB   |                      |
| author   | BIGINT  | → _users(id)         |

### `mixpla__brand_scripts` (join)
| Column         | Type    | Notes                   |
|----------------|---------|-------------------------|
| brand_id       | UUID FK | → mixpla__brands(id)    |
| script_id      | UUID FK | → mixpla__scripts(id)   |
| rank           | INT     | ordering                |
| user_variables | JSONB   |                         |

### `mixpla__scenes`
| Column    | Type    | Notes                   |
|-----------|---------|-------------------------|
| id        | UUID PK |                         |
| script_id | UUID FK | → mixpla__scripts(id)   |
| rank      | INT     | ordering within script  |
| loc_name  | JSONB   |                         |

## Key Joins

```sql
-- Brands with their scripts (ordered)
SELECT b.slug_name, s.id AS script_id, bs.rank
FROM mixpla__brands b
JOIN mixpla__brand_scripts bs ON bs.brand_id = b.id
JOIN mixpla__scripts s ON s.id = bs.script_id
ORDER BY b.slug_name, bs.rank;

-- Scripts with their scenes (ordered)
SELECT s.id AS script_id, sc.id AS scene_id, sc.rank
FROM mixpla__scripts s
JOIN mixpla__scenes sc ON sc.script_id = s.id
ORDER BY s.id, sc.rank;

-- Full brand → script → scene chain
SELECT b.slug_name, bs.rank AS script_rank, sc.rank AS scene_rank, sc.id AS scene_id
FROM mixpla__brands b
JOIN mixpla__brand_scripts bs ON bs.brand_id = b.id
JOIN mixpla__scripts s ON s.id = bs.script_id
JOIN mixpla__scenes sc ON sc.script_id = s.id
ORDER BY b.slug_name, bs.rank, sc.rank;
```
