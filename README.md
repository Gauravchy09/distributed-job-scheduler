# Distributed Job Scheduler

A Spring Boot service that accepts jobs over a REST API, persists them, executes them at the right
time, retries them when they fail, and does all of this correctly **even when several instances of
the service are running at once**.

The interesting problem here is not the CRUD. It is this: if two identical instances both poll the
same table every five seconds, what stops them from picking up the same job and running it twice?
This project answers that with one idea —

> **The database is both the queue and the lock manager.** Coordination between instances happens
> through PostgreSQL row-level locks, not through a separate coordination service.

---

## Status

**In active development.** This README describes the target design; the sections below are the
specification I am building against, not a claim that every piece is finished. See
[`learning/PLAN.md`](learning/PLAN.md) for the day-by-day build log.

| Week | Scope | State |
|---|---|---|
| 1 | Flyway migrations, JPA entities, submit + get endpoints, validation, Docker Compose | 🔨 in progress |
| 2 | Poll loop, `SKIP LOCKED` claim, async execution, handler registry | ⬜ not started |
| 3 | Retry/backoff, dead-letter, stale-lock reaper, idempotency, execution history | ⬜ not started |
| 4 | Cron scheduling, Redis caching, Actuator metrics, Testcontainers suite | ⬜ not started |

Infrastructure (Postgres 16 + Redis 7 via Compose) and the Maven dependency set are in place.

---

## Why this exists

Most "job scheduler" tutorials run a single process with `@Scheduled` and stop there. That works
until you need a second instance for availability, at which point every job fires twice and the
design quietly breaks.

The scope here is deliberately narrow — no Kafka, no leader election, no UI — because the value is
in getting the concurrency, failure and recovery semantics right and being able to explain them,
not in surface area.

---

## Architecture

```
   Client
     │  POST /api/v1/jobs
     ▼
┌─────────────────────────────────────────┐
│  Instance A          Instance B         │   ← identical, both running
│  ┌───────────────┐   ┌───────────────┐  │
│  │ REST API      │   │ REST API      │  │
│  ├───────────────┤   ├───────────────┤  │
│  │ Job Service   │   │ Job Service   │  │
│  ├───────────────┤   ├───────────────┤  │
│  │ Scheduler     │   │ Scheduler     │  │   ← polls every 5s
│  │ Loop          │   │ Loop          │  │
│  ├───────────────┤   ├───────────────┤  │
│  │ Worker Pool   │   │ Worker Pool   │  │   ← thread pool executes
│  └───────┬───────┘   └───────┬───────┘  │
└──────────┼───────────────────┼──────────┘
           │                   │
           ▼                   ▼
      ┌─────────────────────────────┐
      │      PostgreSQL             │   ← single source of truth
      │  jobs / job_executions      │      + the coordination point
      └─────────────────────────────┘
           ▲
      ┌────┴────┐
      │  Redis  │   ← cache for status reads only
      └─────────┘
```

Instances are stateless and interchangeable. Redis caches status reads only — it is never part of
the correctness story, and removing it would cost latency, not correctness.

---

## Quick start

```bash
git clone https://github.com/Gauravchy09/distributed-job-scheduler.git
cd distributed-job-scheduler

docker compose up -d          # Postgres 16 + Redis 7
./mvnw spring-boot:run        # app on :8080
```

Submit a job that runs immediately:

```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
        "name": "send-welcome-email",
        "type": "EMAIL",
        "payload": { "to": "user@example.com", "template": "welcome" },
        "maxAttempts": 3,
        "idempotencyKey": "welcome-user-8891"
      }'
```

Watch it execute:

```bash
curl http://localhost:8080/api/v1/jobs/{id}/executions
```

To see the coordination actually work, run a second instance against the same database and submit a
batch of jobs:

```bash
SERVER_PORT=8081 ./mvnw spring-boot:run
```

Every job should appear exactly once in `job_executions`.

---

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/v1/jobs` | Submit a job |
| `GET` | `/api/v1/jobs/{id}` | Job detail and current status |
| `GET` | `/api/v1/jobs?status=&page=&size=` | Paginated list |
| `DELETE` | `/api/v1/jobs/{id}` | Cancel (only while `PENDING`) |
| `GET` | `/api/v1/jobs/{id}/executions` | Attempt history |
| `POST` | `/api/v1/jobs/{id}/replay` | Re-queue a `DEAD` job |
| `GET` | `/actuator/health`, `/actuator/metrics` | Ops |

**Submit request**

```json
{
  "name": "send-welcome-email",
  "type": "EMAIL",
  "payload": { "to": "user@example.com", "template": "welcome" },
  "runAt": "2026-08-20T09:00:00Z",
  "cronExpression": null,
  "maxAttempts": 3,
  "idempotencyKey": "welcome-user-8891"
}
```

Exactly one of `runAt` or `cronExpression` may be set; both null means *run now*. Requests are
validated with Bean Validation, and failures come back as RFC 7807 problem responses from a
`@RestControllerAdvice`:

```json
{
  "type": "https://example.com/probs/validation",
  "title": "Validation failed",
  "status": 400,
  "detail": "runAt and cronExpression are mutually exclusive"
}
```

Submitting the same `idempotencyKey` twice returns the original job with `200` rather than creating
a second one with `201`.

---

## Data model

**`jobs`** — one row per job, carrying its own schedule, retry counters and lock fields
(`locked_by`, `locked_at`).

**`job_executions`** — one row per *attempt*, with `worker_id`, timings and any error. History,
debugging and metrics fall out of this table for free rather than needing separate instrumentation.

```sql
CREATE INDEX idx_jobs_poll ON jobs (status, next_run_at)
  WHERE status = 'PENDING';
CREATE UNIQUE INDEX idx_jobs_idem ON jobs (idempotency_key)
  WHERE idempotency_key IS NOT NULL;
CREATE INDEX idx_jobs_stale ON jobs (locked_at) WHERE status = 'RUNNING';
CREATE INDEX idx_exec_job ON job_executions (job_id, attempt_number);
```

Schema is managed by Flyway migrations. `ddl-auto: update` is off — in a project meant to look
production-shaped, letting Hibernate mutate the schema is the wrong default.

### State machine

```
                  ┌──────────► CANCELLED
                  │
  PENDING ──► RUNNING ──► SUCCEEDED       (one-time job, done)
     ▲            │
     │            ├──► PENDING            (failed, retries remain — backoff applied)
     │            │
     │            └──► DEAD               (failed, retries exhausted)
     │
     └──────────────────────               (recurring job: after success,
                                            next_run_at recomputed from cron)
```

---

## Design decisions

This is the section worth reading.

### Why database polling instead of Kafka or RabbitMQ

A broker is the right answer at high throughput, but it is the wrong answer here. Jobs in this
system need a queryable status, a cancel operation, an attempt history and a delayed or recurring
schedule — all natural in a table, all awkward in a log-based broker. Adding Kafka would mean
running a second stateful system *and* still keeping Postgres for job state, so it buys complexity
before it buys anything else.

Polling a single indexed table is comfortable into the low thousands of jobs per minute. The point
to switch is when lock contention on the poll query starts showing up in latency — and even then
I would partition by job type across queues before reaching for a broker.

### How duplicate execution is prevented

The naive version has a race: both instances `SELECT` the due rows before either one `UPDATE`s them.

```sql
SELECT * FROM jobs
WHERE status = 'PENDING' AND next_run_at <= now()
ORDER BY next_run_at
LIMIT 20
FOR UPDATE SKIP LOCKED;
```

`FOR UPDATE` locks the rows the transaction reads. `SKIP LOCKED` tells a second transaction to step
*over* rows already locked instead of blocking on them. Instance A takes jobs 1–20, instance B skips
those and takes 21–40. No duplicates, and no instance waiting on another.

The claim runs in a **short transaction** that marks the rows `RUNNING`, stamps `locked_by` and
`locked_at`, then commits. **Execution happens after that transaction commits, never inside it** —
holding a database transaction open for the thirty seconds a job takes to run is exactly the failure
this design exists to avoid.

### Retry strategy, and why jitter

```
attempt_count++
if attempt_count >= max_attempts  → status = DEAD
else → status = PENDING,
       next_run_at = now + min(base * 2^(attempt_count - 1), cap) + jitter
```

Base 1s, cap 5 minutes, jitter a random 0–20%.

Jitter is not decoration. Without it, a thousand jobs that failed together because one downstream
dependency was down will all retry at the same instant and knock it over again — the thundering herd
problem. Randomising the delay spreads the recovery load out.

The policy lives in its own `RetryPolicy` class rather than inline in the scheduler, which keeps the
unit test trivial and the scheduler readable.

### Why a partial index on the poll query

`idx_jobs_poll` covers only `WHERE status = 'PENDING'`. The polling query runs every five seconds
and filters on a status that most rows in a mature table will *not* have — succeeded and dead jobs
accumulate forever. Indexing only the live subset keeps the index small enough to stay hot in memory
and avoids paying to index rows the hot query will never look at.

### At-least-once, not exactly-once

Worth being direct about: this system gives **at-least-once** execution.

A job can run twice if a worker completes its side effect and then dies before committing the status
update. Genuine exactly-once would require the side effect and the status write to share a
transaction, which is impossible when the side effect is an outbound HTTP call. Handlers should
therefore be written to be idempotent. The API's `idempotencyKey` prevents duplicate *submission* —
a different and weaker guarantee than preventing duplicate *execution*.

Idempotent submission is enforced by the database, not by application code: attempt the insert, then
handle the constraint violation. Check-then-insert would just move the same race one layer up.

### Pluggable handlers

```java
public interface JobHandler {
    String type();
    void execute(JobContext context) throws Exception;
}
```

Spring injects every implementation as a `List<JobHandler>`; a registry builds a
`Map<String, JobHandler>` at startup keyed by `type()`. Adding a job type means adding one class and
touching nothing in the scheduler — the strategy pattern doing what it is actually good for.

Three handlers exist to exercise the system: an HTTP webhook caller, a mock email sender, and a
deliberately flaky one that fails at random, so retry and dead-lettering are observable rather than
theoretical.

---

## Configuration

Tunables are bound via `@ConfigurationProperties` rather than hardcoded:

| Property | Default | Meaning |
|---|---|---|
| `scheduler.poll-interval` | `5s` | How often each instance polls for due jobs |
| `scheduler.batch-size` | `20` | Rows claimed per poll |
| `scheduler.visibility-timeout` | `5m` | After this, a `RUNNING` job is treated as orphaned |
| `scheduler.worker-pool-size` | `10` | Bounded execution thread pool per instance |
| `scheduler.retry.base` / `.cap` | `1s` / `5m` | Backoff bounds |

---

## Tests

Five tests carry the weight of this project. Coverage percentage is not the goal.

| Test | What it proves |
|---|---|
| Concurrency | Two threads claiming from a seeded table; every job claimed exactly once, no overlap |
| Retry math | `RetryPolicy` produces the expected backoff sequence and respects the cap |
| Dead-lettering | A permanently failing job reaches `DEAD` after exactly `max_attempts` |
| Idempotency | The same key submitted twice yields one row and two identical responses |
| Stale recovery | A `RUNNING` job with an old `locked_at` is reclaimed by the reaper |

These run against real PostgreSQL via **Testcontainers**, not H2. That is deliberate: `SKIP LOCKED`
does not exist in H2, so an in-memory database would pass silently while testing nothing about the
one behaviour this project depends on.

---

## Limitations, and what changes at 100x

- **Polling contention.** Every instance queries the same table on the same interval. At high
  instance counts the poll itself becomes the bottleneck; the fix is partitioning by job type or
  hash range so instances contend over disjoint row sets.
- **API and workers share a process.** Fine at this size, wrong later — a saturated job pool should
  not degrade API latency. Splitting them lets each scale on its own signal.
- **No priority.** Jobs are ordered strictly by `next_run_at`, so an urgent job queues behind a
  backlog.
- **Single database.** Postgres is the availability ceiling. Sharding by tenant or job type is the
  first step past it.
- **Cron across DST.** Recurring schedules are computed in UTC; local-time recurrence over DST
  transitions is a genuinely harder problem and is not solved here.
- **At-least-once only**, as described above.

---

## Stack

Java 17 · Spring Boot 3 · Spring Web / Data JPA / Validation / Actuator · PostgreSQL 16 · Flyway ·
Redis 7 · Micrometer · Testcontainers · JUnit 5 · Docker Compose

## Layout

```
com.gaurav.distributed_job_scheduler
├── api          controllers, DTOs, exception handling
├── domain       Job, JobExecution, JobStatus
├── repository   Spring Data repositories incl. the claim query
├── service      JobService, SchedulerService, JobExecutor,
│                RetryPolicy, CronService, StaleJobReaper
├── handler      JobHandler, HandlerRegistry, impl/
└── config       AsyncConfig, RedisConfig, SchedulerProperties
```
