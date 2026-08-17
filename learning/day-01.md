# Day 1 — The Thesis: the database *is* the queue and the lock manager

**Time:** ~2.5 hrs · **Code written:** `docker-compose.yml` only · **Java written:** none

Today has no Java in it on purpose. If you cannot explain, at a SQL prompt, exactly why two
instances don't run the same job twice, then no amount of Spring code will save you in an
interview. Today you *see it happen* with your own eyes in two terminals.

---

## Part 0 — What are we actually building? (10 min)

A service that accepts a job over HTTP, remembers it, and runs it at the right moment —
now, at 09:00 next Tuesday, or every hour forever. It retries failures with backoff and
gives up eventually. And it does all of that **correctly when several copies of the service
are running at the same time**.

That last clause is the entire project. Strip it away and this is a weekend CRUD app.

### Why would several copies be running?

Because that's how you deploy anything real:

- **Availability** — one instance dies, requests keep being served.
- **Throughput** — one JVM's thread pool is a ceiling; more instances raise it.
- **Deploys** — rolling deploys mean old and new versions run *simultaneously* for a while.

So "multiple instances" isn't an exotic scaling requirement you bolt on later. It is the
*default state* of a deployed service. A scheduler that only works with one instance is a
scheduler that breaks on every deploy.

### The failure this creates

Every instance runs the same code, including the poll loop:

```
every 5 seconds:
    jobs = SELECT * FROM jobs WHERE status='PENDING' AND next_run_at <= now() LIMIT 20
    for each job: mark RUNNING, then execute it
```

Instance A and Instance B both wake at 15:00:00.000. Both run that `SELECT`. **Both get the
same 20 rows** — because a plain `SELECT` in Postgres takes no locks and does not block
anything. Both then mark them RUNNING. Both execute them.

The customer gets charged twice. The email goes out twice. The report is generated twice.

> **Say this out loud:** *"The bug isn't that two instances read the same rows. It's that
> reading and claiming were two separate steps, and another transaction can slip in between
> them. This is a check-then-act race — the same shape as `if (!map.containsKey(k)) map.put(k,v)`
> in concurrent code."*

That sentence — *check-then-act* — is the general pattern. Recognising it by name is the
difference between "I fixed a bug" and "I understand concurrency."

---

## Part 1 — Why Postgres and not Kafka / RabbitMQ / SQS? (25 min)

You **will** be asked this. "Why didn't you use Kafka?" is bait: they want to know if you
choose technology by fashion or by reasoning. There is a correct answer and it is not
"Kafka was overkill."

### Read the requirement first

| What we need | Does a log-based broker (Kafka) give it? |
|---|---|
| Run this job **at 09:00 next Tuesday** | ✗ Kafka has no native delay. You'd need delay topics or an external timer wheel |
| **Cancel** a job that hasn't run yet | ✗ A log is append-only. You cannot un-publish message #4,812 |
| **Query** "show me all failed jobs from yesterday" | ✗ Not a query engine. You'd need a database anyway |
| **Update** a job's retry count in place | ✗ Same reason — immutable log |
| Retry with **exponential backoff** | ~ Possible, but it's re-publishing to delay topics; awkward |
| Deliver work to exactly one consumer | ✓ Yes, this it does well |

Kafka solves *one* of six requirements. And note the third row: even if you used Kafka,
**you would still need a database** to answer status queries. So the real comparison is not
"Kafka vs Postgres" — it's "**Postgres**" vs "**Postgres plus Kafka plus the operational
burden of Kafka**."

> **The line to say:** *"A scheduled job is mutable, queryable state with a due time — that's
> a database row, not a log entry. A broker gives you delivery; it doesn't give you 'run this
> at 9am, and let me cancel it, and tell me why attempt 2 failed.' Since I need the database
> regardless, adding a broker would mean running two systems and keeping them consistent with
> each other. So the interesting question became: can Postgres also be the queue? It can —
> `SELECT ... FOR UPDATE SKIP LOCKED` is a job-queue primitive, and it's been in Postgres
> since 9.5 precisely because people were using tables as queues."*

### The honest limit — know the number

Do not claim Postgres scales forever. Know where it breaks and why:

- **Latency floor.** Polling every 5s means average pickup delay ≈ 2.5s, worst case 5s. If
  the product needs sub-100ms dispatch, polling is the wrong shape (you'd add `LISTEN/NOTIFY`
  or move to a broker).
- **Write amplification.** Every job costs *several* row updates (claim → RUNNING → SUCCEEDED,
  plus an execution row). Postgres MVCC means each `UPDATE` writes a **new row version** and
  leaves the old one dead, for autovacuum to clean up later. A high-churn queue table is
  autovacuum's least favourite thing.
- **Where it actually falls over:** roughly the **low thousands of jobs/second** on one
  well-tuned instance. Not query speed — the claim query is a sub-millisecond index scan —
  but **dead-tuple churn and vacuum pressure**.

> **The scaling answer:** *"I'd stay on Postgres to the order of a thousand jobs a second.
> The first thing to break is autovacuum keeping up with row churn, not query latency. Before
> reaching for Kafka I'd take cheaper steps first: partition the table by status or job type,
> shorten the poll interval with `LISTEN/NOTIFY` for immediate jobs, and split API instances
> from worker instances. I'd move to a broker when the workload becomes high-volume streaming
> rather than scheduled tasks — because at that point I'm no longer scheduling, I'm streaming,
> and that's a different problem."*

---

## Part 2 — The database concepts you need (35 min)

Read this before the lab. Every term here is fair game in an interview.

### 2.1 MVCC — Multi-Version Concurrency Control

Postgres never overwrites a row in place. An `UPDATE` writes a **new version** of the row and
marks the old version as expired. Each version carries two hidden system columns:

- `xmin` — the transaction id that created this version
- `xmax` — the transaction id that deleted/superseded it (0 if still live)

Every statement runs against a **snapshot**: a set of "which transactions had committed at the
moment I started." A row version is visible to you if its `xmin` committed before your snapshot
and its `xmax` hadn't.

The consequence is the single most important sentence about Postgres concurrency:

> **Readers never block writers, and writers never block readers.**

A reader just walks past versions it can't see. This is why the naive poll doesn't error out —
it happily succeeds in both instances. **MVCC makes the race silent.** Nothing crashes; you
simply run the job twice and find out from a customer.

But:

> **Writers *do* block writers — on the same row.**

If A updates row 7 and hasn't committed, B's update of row 7 waits until A commits or rolls
back. That row-level exclusive lock is the primitive we're going to hijack.

### 2.2 `SELECT ... FOR UPDATE` — reading like a writer

`FOR UPDATE` says: *take the same row-level exclusive lock an `UPDATE` would take, but don't
write anything yet.* It's how you say "I intend to modify these rows, hands off."

Postgres has four row-lock strengths, weakest to strongest:

| Mode | Meaning |
|---|---|
| `FOR KEY SHARE` | weakest; what a foreign-key check takes |
| `FOR SHARE` | "I'm reading this and it must not change" |
| `FOR NO KEY UPDATE` | what a plain `UPDATE` of non-key columns takes |
| `FOR UPDATE` | strongest; blocks everything above |

We want `FOR UPDATE`. Being able to name that there *is* a hierarchy is a nice depth signal.

**Critically: row locks are held until the transaction ends** — `COMMIT` or `ROLLBACK`. There
is no "unlock" statement. This fact drives a design decision on Day 6: if execution happened
inside the claim transaction, a 30-second job would hold its row lock for 30 seconds, and hold
a database connection out of the pool for 30 seconds. So the claim transaction must be short
and execution must happen **after it commits**.

### 2.3 The three ways to handle a locked row

```sql
SELECT ... FOR UPDATE;              -- wait for the lock (default)
SELECT ... FOR UPDATE NOWAIT;       -- raise an error immediately
SELECT ... FOR UPDATE SKIP LOCKED;  -- pretend those rows don't exist, move on
```

`SKIP LOCKED` was added in Postgres 9.5 **specifically because people were building job queues
on tables**. It changes the query's meaning from "give me the due jobs" to "**give me due jobs
that nobody else is currently working on**" — which is exactly what a worker wants.

Two details worth knowing, because they're the follow-up questions:

1. **`SKIP LOCKED` makes the result set non-deterministic**, and that is fine here. You don't
   care *which* 20 jobs you get, only that nobody else got them too.
2. **`LIMIT` counts only rows you actually locked.** Skipped rows don't consume the limit —
   Postgres keeps scanning to fill the batch. That's why A gets jobs 1–20 and B gets 21–40,
   rather than B getting nothing.

### 2.4 Isolation levels — and a trap

| Level | Postgres behaviour |
|---|---|
| `READ UNCOMMITTED` | accepted but behaves as `READ COMMITTED` — Postgres has no dirty reads |
| **`READ COMMITTED`** | **the default.** A fresh snapshot per *statement* |
| `REPEATABLE READ` | one snapshot for the whole *transaction* |
| `SERIALIZABLE` | as if transactions ran one at a time; may abort with serialization errors |

**Keep the claim query in `READ COMMITTED`.** Here's the trap you'll demonstrate in Lab 4:

In `REPEATABLE READ` or `SERIALIZABLE`, if a row you're about to lock has been changed by
another transaction since yours started, Postgres **aborts you** with
`could not serialize access due to concurrent update`. And `SKIP LOCKED` does *not* rescue
you — it only skips rows that are *currently locked*, not rows that were *already updated and
committed*. A stronger isolation level would make your scheduler throw errors under load.

> **A rare and impressive point:** *"Stronger isolation is not 'safer' by default. For the
> claim query, `READ COMMITTED` plus an explicit row lock is both correct and non-blocking,
> whereas `REPEATABLE READ` would abort workers under contention. I chose the lock, not the
> isolation level, because the lock is the precise tool for this."*

### 2.5 `READ COMMITTED` re-checking (the subtle bit)

In `READ COMMITTED`, when your `FOR UPDATE` blocks on someone else's lock and they then commit,
Postgres doesn't just hand you the stale row. It **re-fetches the newest version and re-runs
your `WHERE` clause against it**. If the row no longer matches, it's dropped from your results.

This is why the plain blocking `FOR UPDATE` version is *correct but slow* — B waits for A, then
discovers A already claimed those rows, and moves on. `SKIP LOCKED` gets to the same answer
without the waiting. You'll watch both happen in Lab 2 and Lab 3.

---

## Part 3 — Lab: prove it yourself (60 min)

### Setup

```powershell
cd D:\distributed-job-scheduler\distributed-job-scheduler
docker compose up -d
docker compose ps          # both services should say (healthy)
```

Now open **two separate terminals**. In each:

```powershell
docker compose exec postgres psql -U scheduler -d scheduler
```

Call them **T1** and **T2**. Give each a visible identity so you never lose track:

```sql
-- run in T1
\set PROMPT1 'T1 %/%R%# '
-- run in T2
\set PROMPT1 'T2 %/%R%# '
```

Seed a throwaway table (**not** the real schema — that's Day 2, via Flyway):

```sql
-- T1
CREATE TABLE jobs_demo (
  id          int PRIMARY KEY,
  status      text NOT NULL DEFAULT 'PENDING',
  next_run_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO jobs_demo (id) SELECT generate_series(1, 10);
SELECT * FROM jobs_demo ORDER BY id;
```

A helper you'll use to reset between labs:

```sql
UPDATE jobs_demo SET status = 'PENDING';
```

---

### Lab 1 — Watch the race happen

```sql
-- T1
BEGIN;
SELECT id, status FROM jobs_demo
WHERE status = 'PENDING' AND next_run_at <= now()
ORDER BY id LIMIT 3;
```

```sql
-- T2   (T1 is still open — do NOT commit it)
BEGIN;
SELECT id, status FROM jobs_demo
WHERE status = 'PENDING' AND next_run_at <= now()
ORDER BY id LIMIT 3;
```

**Both return 1, 2, 3.** Neither blocked. Neither errored. This is the bug, and notice how
comfortable it looks. Finish the story:

```sql
-- T1
UPDATE jobs_demo SET status='RUNNING' WHERE id IN (1,2,3);
COMMIT;
```
```sql
-- T2
UPDATE jobs_demo SET status='RUNNING' WHERE id IN (1,2,3);
COMMIT;
```

T2's `UPDATE` blocked briefly, then succeeded. Both instances now believe they own jobs 1–3.
Both would execute them.

✍️ **Write in your own words below what just happened and why nothing complained.**

> _your answer:_

Reset: `UPDATE jobs_demo SET status='PENDING';`

---

### Lab 2 — `FOR UPDATE`: correct, but everyone queues

```sql
-- T1
BEGIN;
SELECT id, status FROM jobs_demo
WHERE status = 'PENDING' ORDER BY id LIMIT 3
FOR UPDATE;
-- returns 1,2,3
```

```sql
-- T2
BEGIN;
SELECT id, status FROM jobs_demo
WHERE status = 'PENDING' ORDER BY id LIMIT 3
FOR UPDATE;
-- ⏳ HANGS. This is the point of the lab — sit with it.
```

While T2 hangs, open a **third** terminal and look at the blocking from the outside:

```powershell
docker compose exec postgres psql -U scheduler -d scheduler
```
```sql
SELECT pid, state, wait_event_type, wait_event, left(query, 50) AS q
FROM pg_stat_activity
WHERE datname = 'scheduler' AND pid <> pg_backend_pid();

SELECT locktype, mode, granted, relation::regclass
FROM pg_locks WHERE NOT granted;
```

You should see T2 with `wait_event_type = Lock`. **You just watched one transaction wait on
another's row lock.** Now release it:

```sql
-- T1
UPDATE jobs_demo SET status='RUNNING' WHERE id IN (1,2,3);
COMMIT;
```

T2 immediately unblocks and returns **4, 5, 6** — *not* 1,2,3. (Measured: T2 sat blocked for
~6 seconds, exactly the remaining lifetime of T1's transaction.) That is §2.5 in action: T2 was
handed the fresh row versions, re-evaluated `status='PENDING'`, found 1–3 no longer matched,
dropped them, and kept scanning until the limit was filled.

> Correct — no duplicates. But T2 sat idle for the entire duration of T1's transaction. With
> ten instances, nine of them are queueing behind one. **That's the problem `SKIP LOCKED` solves.**

```sql
-- T2
COMMIT;
```
Reset: `UPDATE jobs_demo SET status='PENDING';`

---

### Lab 3 — `SKIP LOCKED`: the actual answer ⭐

```sql
-- T1
BEGIN;
SELECT id, status FROM jobs_demo
WHERE status = 'PENDING' ORDER BY id LIMIT 3
FOR UPDATE SKIP LOCKED;
-- 1,2,3
```
```sql
-- T2
BEGIN;
SELECT id, status FROM jobs_demo
WHERE status = 'PENDING' ORDER BY id LIMIT 3
FOR UPDATE SKIP LOCKED;
-- 4,5,6 — INSTANTLY. No waiting.
```

Run it a third time in your third terminal: you'll get **7, 8, 9**. Three workers, three
disjoint batches, zero coordination service, zero blocking.

**This is the project.** Everything else is plumbing around this behaviour.

Now prove locks are transaction-scoped:

```sql
-- T1
ROLLBACK;
```
```sql
-- T2  (or the third session)
SELECT id FROM jobs_demo WHERE status='PENDING' ORDER BY id LIMIT 3
FOR UPDATE SKIP LOCKED;
```
Jobs 1–3 are claimable again. **Nothing was lost.** If a worker's transaction dies before
committing, its rows return to the pool automatically. (Note carefully: this covers a crash
*during the claim*. A crash *after* the claim commits leaves a row stuck in RUNNING — that's
a different problem, and it's why Day 9 exists.)

```sql
COMMIT;  -- or ROLLBACK, in every open session
```
Reset: `UPDATE jobs_demo SET status='PENDING';`

---

### Lab 3b — ⚠️ `SKIP LOCKED` alone is **not** enough

The most common misunderstanding about this pattern. In Lab 3 the batches were disjoint only
because all three transactions were **open at the same time**. Row locks vanish at `COMMIT`.
So run two claims *one after another*:

```sql
BEGIN; SELECT id FROM jobs_demo WHERE status='PENDING' ORDER BY id LIMIT 3
       FOR UPDATE SKIP LOCKED; COMMIT;
-- 1, 2, 3

BEGIN; SELECT id FROM jobs_demo WHERE status='PENDING' ORDER BY id LIMIT 3
       FOR UPDATE SKIP LOCKED; COMMIT;
-- 1, 2, 3   ← THE SAME ROWS
```

Verified output:
```
1 2 3   <- txn A
1 2 3   <- txn B   (SAME ROWS!)
```

Of course — the first transaction released its locks and never changed anything, so the rows
are still `PENDING` and still due. **The lock only protects a row for the lifetime of the
transaction that holds it.** What makes a claim durable is *writing the status change before
committing*:

```sql
BEGIN;
UPDATE jobs_demo SET status = 'RUNNING'
WHERE id IN (
  SELECT id FROM jobs_demo WHERE status='PENDING'
  ORDER BY id LIMIT 3 FOR UPDATE SKIP LOCKED
)
RETURNING id;
COMMIT;
```
```
2 3 1   <- txn A     (note: RETURNING order is not guaranteed)
4 5 6   <- txn B     disjoint ✓
```

> **The correct mental model:** `SKIP LOCKED` prevents two workers from claiming the same row
> **at the same time**. Flipping `status` to `RUNNING` inside that same transaction is what
> prevents the row being claimed **again later**. You need both. The lock gives you mutual
> exclusion; the status write gives you durability of the claim.

This is exactly why the spec says the claim transaction *"marks rows RUNNING and sets
`locked_by` / `locked_at`, then commits."* Day 6 is where you write it.

---

### Lab 4 — The isolation-level trap

```sql
-- T1
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM jobs_demo;   -- forces the snapshot to be taken NOW
```
```sql
-- T2  (autocommit, no BEGIN)
UPDATE jobs_demo SET status='RUNNING' WHERE id = 1;
```
```sql
-- T1
SELECT id FROM jobs_demo WHERE status='PENDING' ORDER BY id LIMIT 3
FOR UPDATE SKIP LOCKED;
```

```
ERROR:  could not serialize access due to concurrent update
```

**`SKIP LOCKED` did not save you.** Row 1 wasn't locked — T2 had already committed. Under
`REPEATABLE READ`, discovering that a row you want to lock changed after your snapshot is a
fatal error. Under `READ COMMITTED` the same sequence quietly returns rows 2, 3, 4.

Verify that claim yourself — rerun with `BEGIN;` instead of `BEGIN ISOLATION LEVEL REPEATABLE READ;`.

```sql
ROLLBACK;
```

---

### Lab 5 — See the plan

```sql
UPDATE jobs_demo SET status='PENDING';
EXPLAIN (ANALYZE, BUFFERS)
SELECT id FROM jobs_demo WHERE status='PENDING' AND next_run_at <= now()
ORDER BY id LIMIT 3 FOR UPDATE SKIP LOCKED;
```

Verified output on this machine:

```
 Limit  (cost=1.29..1.33 rows=3 width=10) (actual time=0.076..0.080 rows=3 loops=1)
   ->  LockRows  (cost=1.29..1.40 rows=9 width=10) (actual time=0.075..0.078 rows=3 loops=1)
         ->  Sort  (cost=1.29..1.31 rows=9 width=10) (actual rows=3 loops=1)
               Sort Key: id
               ->  Seq Scan on jobs_demo  (actual time=0.012..0.015 rows=10 loops=1)
                     Filter: ((status = 'PENDING'::text) AND (next_run_at <= now()))
```

Read it bottom-up, the way Postgres executes it: scan → sort → **lock** → limit. The
**`LockRows` node sits *below* `Limit`** — that is the physical reason skipped rows don't
consume the limit. Rows that fail to lock are dropped by `LockRows` and never reach `Limit`,
so `Limit` keeps pulling until it has 3 rows it actually holds.

Being able to point at that plan and say *"`LockRows` is below `Limit`, that's why B gets
21–40 and not an empty set"* is a level of specificity almost nobody brings.

On 10 rows you get a `Seq Scan` — correct behaviour, since scanning 10 rows is cheaper than
an index lookup. Day 2 loads 100k rows and shows the partial index taking over.

---

## Part 4 — Self-check

Answer out loud, no notes. If you stumble, reread that section.

1. Two instances poll at the same millisecond. Walk through what happens, statement by statement.
2. Why doesn't the naive `SELECT` throw an error, deadlock, or block?
3. What is a check-then-act race? Give an example outside databases.
4. What exactly does `FOR UPDATE` lock, and when is that lock released?
5. `FOR UPDATE` alone is already correct. So why add `SKIP LOCKED`?
6. Instance A holds jobs 1–20. Instance B runs the same query with `LIMIT 20`. How many rows does B get, and why not zero?
7. Would `REPEATABLE READ` be safer? Defend your answer.
8. Why can't execution happen inside the claim transaction? Give two distinct reasons.
9. Give the strongest version of "why not Kafka" in under 45 seconds.
10. At what scale does this design break, and what breaks *first*?

---

## Part 5 — What you wrote today

- `docker-compose.yml` — Postgres 17 + Redis 7, with healthchecks and `log_lock_waits=on`

## Tomorrow (Day 2)

The schema, and why every column and index is shaped the way it is. Flyway migrations
(and why `ddl-auto: update` is a red flag). Partial indexes proved with `EXPLAIN ANALYZE`
on 100k rows. The state machine.

Leave `docker compose` running — Day 2 starts where this ends.
