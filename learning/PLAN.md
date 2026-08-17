# 14-Day Build & Learn Plan

**Format:** 2–3 hrs/day. Every day is *concept first, code second*. Each day has its own file.

**The rule (from spec §12):** four things you write yourself, no generation —
the claim query + its transaction (Day 6), `RetryPolicy` (Day 8), the reaper (Day 9),
the concurrency test (Day 11). Those days are marked ✍️. Everything else I scaffold.

---

## Week 1 — Foundations and the scheduler

| Day | Theme | Concepts (learn first) | Build (code second) |
|---|---|---|---|
| **1** | The thesis | Why a job scheduler; the duplicate-execution problem; DB-as-queue vs Kafka; MVCC, isolation levels, row locks, `FOR UPDATE`, `SKIP LOCKED` | Docker Compose (Postgres + Redis); prove `SKIP LOCKED` by hand in two `psql` sessions |
| **2** | Data model | Why this schema; JSONB vs columns; `TIMESTAMPTZ` vs `TIMESTAMP`; B-tree indexes; partial indexes; the state machine | Flyway migrations for `jobs` + `job_executions`; prove the partial index with `EXPLAIN ANALYZE` |
| **3** | Persistence | JPA/Hibernate: entity lifecycle, dirty checking, flush, N+1, lazy loading, optimistic vs pessimistic locking | `Job`, `JobExecution`, `JobStatus` entities; repositories; connection pool (HikariCP) tuning |
| **4** | The API | REST resource design; DTO-vs-entity boundary; Bean Validation; RFC-7807 problem details | `POST /jobs`, `GET /jobs/{id}`, list, cancel; `@RestControllerAdvice` |
| **5** | Execution | Strategy pattern; Spring's `List<T>` injection; thread pools — core vs max, bounded queues, rejection policies | `JobHandler` + `HandlerRegistry` + 3 handlers; `AsyncConfig`; `SchedulerProperties` |
| **6** ✍️ | The claim | Why the claim transaction must be short; why execution happens *after* commit; `@Transactional` proxy pitfalls; `REQUIRES_NEW` | **You write** `claimDueJobs` + the claim transaction; `@Scheduled` poll loop |
| **7** | Proof | Consolidation | Run **two instances**, 100 jobs, assert each ran once. Mock interview #1 |

## Week 2 — Failure handling and polish

| Day | Theme | Concepts (learn first) | Build (code second) |
|---|---|---|---|
| **8** ✍️ | Retries | Exponential backoff; the thundering herd; why jitter; full vs equal jitter; dead-letter queues | **You write** `RetryPolicy`; DEAD state; replay endpoint |
| **9** ✍️ | Crash recovery | Visibility timeout; SQS's model; at-least-once delivery; why a heartbeat would be better | **You write** `StaleJobReaper`; kill -9 a worker and watch recovery |
| **10** | Idempotency | Check-then-act races; letting the DB enforce invariants; at-least-once vs exactly-once, honestly | Idempotency keys; constraint-violation handling; graceful shutdown via `@PreDestroy` |
| **11** ✍️ | Tests | Why Testcontainers over H2; test isolation; deterministic concurrency testing | **You write** the concurrency test; + retry math, dead-letter, idempotency, stale recovery |
| **12** | Recurrence & cache | Cron semantics; DST/timezone traps; cache invalidation; why cache reads only | `CronService`; recurring reschedule; Redis `@Cacheable` on status reads |
| **13** | Ops | Metrics that matter (counters vs timers vs gauges); RED method; health checks | Micrometer metrics; actuator; README design-decisions; deploy |
| **14** | Defend it | — | Full mock interview: 90-second + 10-minute versions, 30 min of grilling, fix every gap |

---

## Artifacts we keep

- `learning/day-NN.md` — what you learned that day, in your words plus mine
- `INTERVIEW.md` — the Q&A bank; every design decision becomes a question + answer + likely follow-ups
- `README.md` — written incrementally, especially §5 Design Decisions and §7 Limitations

## The done test (spec §12)

> Could you whiteboard this system from memory, no notes, and answer
> *"what happens if two instances poll at the same millisecond?"* without hesitating?

If yes, it doesn't matter who typed the boilerplate. That question is Day 1's material.
