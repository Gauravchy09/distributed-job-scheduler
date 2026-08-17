# Distributed Job Scheduler — Build Specification

A Spring Boot service that accepts jobs over a REST API, persists them, executes them at the right time, retries them when they fail, and does all of this correctly even when multiple instances of the service are running at once.

This document is the blueprint. Read it fully before writing code, then build section by section.

---

## 1. Scope

**In scope**
- Submit a job to run immediately, at a future time, or on a recurring cron schedule
- Persist jobs so nothing is lost on restart
- Multiple service instances polling the same database without executing a job twice
- Automatic retry with exponential backoff, then a dead-letter state
- Idempotent submission (same request twice creates one job)
- Job status, execution history, and basic metrics
- Recovery from a worker crashing mid-execution

**Deliberately out of scope**
- Kafka / RabbitMQ (database polling is correct at this scale — this is a defensible decision, not a shortcut)
- A UI (the API is the product)
- Authentication (optional; add only if week 4 has spare time)
- Distributed consensus, leader election, multi-region

Keeping scope tight is part of the exercise. A finished small system interviews far better than an abandoned large one.

---

## 2. Architecture

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

**The central idea:** the database is both the queue and the lock manager. Coordination between instances happens through row-level locks, not through a separate coordination service. Understand this sentence well enough to say it out loud — it's the thesis of the whole project.

---

## 3. Data model

### Table: `jobs`

| Column | Type | Notes |
|---|---|---|
| `id` | UUID | primary key |
| `idempotency_key` | VARCHAR(255) | unique, nullable |
| `name` | VARCHAR(255) | human label |
| `type` | VARCHAR(100) | selects which handler runs it |
| `payload` | JSONB | arbitrary job input |
| `cron_expression` | VARCHAR(100) | null = one-time job |
| `status` | VARCHAR(20) | see state machine below |
| `next_run_at` | TIMESTAMPTZ | when it becomes eligible |
| `attempt_count` | INT | starts at 0 |
| `max_attempts` | INT | default 3 |
| `locked_by` | VARCHAR(100) | instance id, nullable |
| `locked_at` | TIMESTAMPTZ | nullable — used for stale lock recovery |
| `last_error` | TEXT | nullable |
| `created_at` | TIMESTAMPTZ | |
| `updated_at` | TIMESTAMPTZ | |

### Table: `job_executions`

| Column | Type | Notes |
|---|---|---|
| `id` | UUID | primary key |
| `job_id` | UUID | FK → jobs.id |
| `attempt_number` | INT | |
| `status` | VARCHAR(20) | SUCCEEDED / FAILED |
| `started_at` | TIMESTAMPTZ | |
| `finished_at` | TIMESTAMPTZ | |
| `duration_ms` | BIGINT | |
| `error_message` | TEXT | nullable |
| `worker_id` | VARCHAR(100) | which instance ran it |

One row per attempt. This gives you history, debugging, and metrics for free.

### Indexes — and why each exists

```sql
CREATE INDEX idx_jobs_poll ON jobs (status, next_run_at)
  WHERE status = 'PENDING';
CREATE UNIQUE INDEX idx_jobs_idem ON jobs (idempotency_key)
  WHERE idempotency_key IS NOT NULL;
CREATE INDEX idx_jobs_stale ON jobs (locked_at) WHERE status = 'RUNNING';
CREATE INDEX idx_exec_job ON job_executions (job_id, attempt_number);
```

The first is a **partial index** — it only covers PENDING rows, which is the only status the polling query cares about. Being able to explain why that's better than a plain composite index is a strong interview moment. Say it like this: the hot query runs every five seconds and filters on a status that most rows don't have, so indexing only that subset keeps the index small and hot in memory.

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

Draw this in your README. Interviewers like state machines because they prove you thought about edge cases rather than just happy paths.

---

## 4. API surface

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/v1/jobs` | submit a job |
| GET | `/api/v1/jobs/{id}` | job detail + current status |
| GET | `/api/v1/jobs?status=&page=&size=` | paginated list |
| DELETE | `/api/v1/jobs/{id}` | cancel (only if PENDING) |
| GET | `/api/v1/jobs/{id}/executions` | attempt history |
| POST | `/api/v1/jobs/{id}/replay` | re-queue a DEAD job |
| GET | `/actuator/health`, `/actuator/metrics` | ops |

**Submit request:**
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

Rules: exactly one of `runAt` or `cronExpression` (null `runAt` + null cron = run now). Validate with `@Valid` and Bean Validation annotations, and return RFC-7807-style problem responses from a `@RestControllerAdvice`.

---

## 5. The five mechanisms that matter

Everything above is scaffolding. These five are what the project is actually about, and what you'll be questioned on.

### 5.1 Safe pickup with `SKIP LOCKED`

Both instances poll the same table every few seconds. The naive version — `SELECT` due jobs, then `UPDATE` them to RUNNING — has a race: both read the same rows before either writes.

The fix:

```sql
SELECT * FROM jobs
WHERE status = 'PENDING' AND next_run_at <= now()
ORDER BY next_run_at
LIMIT 20
FOR UPDATE SKIP LOCKED;
```

`FOR UPDATE` locks the rows the transaction reads. `SKIP LOCKED` tells other transactions to step over already-locked rows instead of blocking on them. Instance A grabs jobs 1–20, Instance B skips those and takes 21–40. No duplicates, no waiting.

In Spring Data JPA:
```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
@Query("SELECT j FROM Job j WHERE j.status = 'PENDING' AND j.nextRunAt <= :now ORDER BY j.nextRunAt")
List<Job> claimDueJobs(@Param("now") Instant now, Pageable pageable);
```
(The `-2` hint maps to SKIP LOCKED. If the hint fights you, use a native query — clarity beats cleverness.)

The claim must happen inside a short transaction that marks rows RUNNING and sets `locked_by` / `locked_at`, then commits. **Execution happens after that transaction commits, not inside it.** Holding a database transaction open while a job runs for thirty seconds is exactly the mistake this design exists to avoid.

### 5.2 Stale lock recovery

A worker marks a job RUNNING and then the process is killed. The row sits RUNNING forever, locked by an instance that no longer exists.

Run a reaper on a schedule: find jobs where `status = 'RUNNING'` and `locked_at < now() - visibility_timeout` (say five minutes), reset them to PENDING, increment `attempt_count`. This is the same reasoning behind SQS visibility timeouts, and saying so shows you understand the pattern generally rather than just your own code.

### 5.3 Retry with exponential backoff

On failure:
```
attempt_count++
if attempt_count >= max_attempts  → status = DEAD
else → status = PENDING,
       next_run_at = now + min(base * 2^(attempt_count - 1), cap) + jitter
```

Base 1s, cap 5 minutes, jitter a random 0–20%. Jitter matters: without it, a thousand jobs that failed together will retry together and hammer the same failing dependency in lockstep. That's the thundering herd problem, and mentioning it unprompted lands well.

Keep the retry policy in its own class (`RetryPolicy`) rather than inline in the scheduler. Separation of concerns, and it makes the unit test trivial.

### 5.4 Idempotency

`idempotency_key` has a unique index. On submit, attempt insert; on constraint violation, look up the existing job and return it with `200` instead of `201`. Do not check-then-insert in application code — that's a race. Let the database enforce it and handle the exception.

Note the distinction in your README: this gives **at-least-once** execution, not exactly-once. A job can run twice if a worker dies after finishing work but before committing status. Exactly-once would require the job's side effect and the status update to be in one transaction, which isn't possible when the side effect is an external HTTP call. Stating this limitation honestly is a much stronger signal than pretending you solved it.

### 5.5 Pluggable handlers

```java
public interface JobHandler {
    String type();
    void execute(JobContext context) throws Exception;
}
```

Spring injects every implementation as a `List<JobHandler>`; build a `Map<String, JobHandler>` at startup keyed by `type()`. Adding a job type means adding one class, no changes to the scheduler.

Write three sample handlers: an HTTP webhook caller, a mock email sender, and a deliberately flaky one that fails randomly so you can watch retries and dead-lettering actually happen.

This is the strategy pattern, and it's the cleanest OOP talking point in the project.

---

## 6. Package structure

```
com.<you>.scheduler
├── SchedulerApplication.java
├── api
│   ├── JobController.java
│   ├── dto/            CreateJobRequest, JobResponse, ExecutionResponse
│   └── GlobalExceptionHandler.java
├── domain
│   ├── Job.java
│   ├── JobExecution.java
│   └── JobStatus.java
├── repository
│   ├── JobRepository.java
│   └── JobExecutionRepository.java
├── service
│   ├── JobService.java          submit / cancel / query
│   ├── SchedulerService.java    the poll loop + claim
│   ├── JobExecutor.java         runs a claimed job, records outcome
│   ├── RetryPolicy.java
│   ├── CronService.java         next-run computation
│   └── StaleJobReaper.java
├── handler
│   ├── JobHandler.java
│   ├── HandlerRegistry.java
│   └── impl/
└── config
    ├── AsyncConfig.java         worker thread pool
    ├── RedisConfig.java
    └── SchedulerProperties.java  poll interval, batch size, timeouts
```

Put tunables (`poll-interval`, `batch-size`, `visibility-timeout`, `worker-pool-size`) in `application.yml` bound via `@ConfigurationProperties`. Hardcoded magic numbers look junior; externalized configuration looks like someone who has operated software.

---

## 7. Stack

- Java 17+, Spring Boot 3.x
- Spring Web, Spring Data JPA, Spring Validation, Spring Actuator
- PostgreSQL 15+
- Flyway for schema migrations (write real SQL migrations — `ddl-auto: update` is a red flag in a project meant to look production-shaped)
- Redis + Spring Cache for status-read caching
- Testcontainers + JUnit 5 for tests
- Docker Compose: app + postgres + redis, one command to run
- Micrometer metrics via Actuator

---

## 8. Four-week build plan

**Week 1 — foundation**
Flyway migrations for both tables. JPA entities. Submit + get-by-id endpoints. Validation and the exception handler. Jobs execute immediately and synchronously — no scheduling yet. Docker Compose running Postgres.
*Done when:* you can POST a job and see it succeed in the executions table.

**Week 2 — the scheduler**
`@Scheduled` poll loop. The `SKIP LOCKED` claim query. Status transitions. Async execution on a bounded thread pool. Handler interface + registry + two handlers.
*Done when:* you run two instances on different ports against one database, submit 100 jobs, and each executes exactly once. **Actually run this test.** It's the core claim of the project and you need to have seen it work.

**Week 3 — failure handling**
Retry policy with backoff and jitter. Dead-letter state. Stale job reaper. Idempotency keys. Execution history endpoint. Graceful shutdown via `@PreDestroy` so in-flight jobs finish before the process exits.
*Done when:* the flaky handler produces visible retry sequences with widening gaps, then lands in DEAD.

**Week 4 — polish**
Cron parsing and recurring reschedule. Redis caching on status reads. Actuator metrics (jobs queued, executed, failed, execution duration timer). Testcontainers integration tests including the concurrency test. README. Deploy to a free tier (Railway, Render, Fly.io).
*Done when:* a stranger can clone it, run `docker compose up`, and submit a job.

If you slip, cut week 4's cron support before cutting week 3's failure handling. Failure handling is what makes it interesting.

---

## 9. Tests worth writing

Don't chase coverage. Write these five:

1. **Concurrency** — two threads claiming from a seeded table; assert every job is claimed exactly once, no overlaps.
2. **Retry math** — unit test on `RetryPolicy` asserting the backoff sequence and the cap.
3. **Dead-lettering** — a job that always fails reaches DEAD after exactly `max_attempts`.
4. **Idempotency** — same key submitted twice yields one row and two identical responses.
5. **Stale recovery** — a RUNNING job with an old `locked_at` gets reclaimed by the reaper.

Use Testcontainers so these run against real Postgres. `SKIP LOCKED` doesn't exist in H2, so an in-memory database would silently not test the thing you most need tested — which is itself a good detail to mention.

---

## 10. README outline

This is half the deliverable. Interviewers read it before they talk to you.

1. One-paragraph description and the problem it solves
2. Architecture diagram
3. Quick start (`docker compose up`, one sample curl)
4. API reference with request/response examples
5. **Design decisions** — the important section:
   - Why database polling instead of Kafka, and the throughput point at which you'd switch
   - How duplicate execution is prevented, with the SQL
   - The retry strategy and why jitter is there
   - Why the partial index
   - At-least-once vs exactly-once, stated honestly
6. State machine diagram
7. **Limitations and what I'd change at 100x scale** — polling contention, partitioning by job type, moving to a broker, sharding, separating API instances from worker instances
8. What's tested and why those tests

Section 5 and 7 are what separate this from a tutorial project. Write them in your own words even if the code had help.

---

## 11. Interview mapping

| Question type | What you use |
|---|---|
| "Walk me through a project" | The whole thing — lead with the multi-instance correctness problem, not the CRUD |
| Concurrency / race conditions | SKIP LOCKED, the claim transaction, why execution is outside it |
| Database design | Partial index, JSONB payload, the executions table, migrations |
| System design | Queue semantics, backoff, dead letters, visibility timeout, scaling path |
| OOP / design patterns | Handler strategy pattern, registry, externalized config |
| "Tell me about a bug you found" (Dive Deep) | The duplicate-execution or stale-lock discovery |
| "A time you simplified" (Invent and Simplify) | Choosing Postgres polling over Kafka, with the reasoning |
| "Highest standards" | Testcontainers over H2 because H2 would have faked the critical behaviour |

Prepare a 90-second version and a 10-minute version of the project explanation. Practice both out loud.

---

## 12. Using AI while building this — read this part twice

You said you'd build this with AI help. That's fine and normal. But the entire value of this project is that you can defend it under questioning, and AI-generated code you don't understand recreates the exact problem you're trying to escape with your copied projects. Amazon interviewers dig into resume claims; a project that collapses under three follow-ups is worse than no project.

**Rules that keep the value intact:**

- **Write these four yourself, by hand, no generation:** the claim query and its transaction, `RetryPolicy`, the reaper, and the concurrency test. They're small. They're also 80% of what you'll be asked about.
- **Ask for explanations before code.** "Explain how SKIP LOCKED behaves when two transactions overlap" before "write me the repository method."
- **Generate at the method level, not the project level.** "Write me a job scheduler" produces something you didn't design. "Write the JPA entity for this schema" is a typing shortcut.
- **After each generated chunk, close it and re-explain it out loud.** If you can't, delete it and write it again slowly.
- **Debug your own failures.** The bugs are where the interview stories come from. Pasting a stack trace and taking the fix costs you the story.
- **Use AI as an interviewer at the end.** Have it grill you on the design for thirty minutes. Note every question you fumble and go fix that understanding.

A practical test before you call it done: could you whiteboard this system from memory, with no notes, and answer "what happens if two instances poll at the same millisecond?" without hesitating? If yes, it doesn't matter who typed the boilerplate. If no, you have more work regardless of whether the code runs.
