# Interview Q&A Bank

Every design decision in this project, written as the question an interviewer will actually
ask, the answer in my own words, and the follow-ups they'll dig into.

Rule for using this file: **read the question, answer out loud, then check.** Reading the
answers passively builds recognition, not recall — and interviews test recall.

Legend: 🔴 = they will definitely ask this · 🟡 = likely · ⚪ = depth bonus

---

## Section 1 — The core problem (Day 1)

### 🔴 Q1.1 "Walk me through this project."

**90-second version.** *"It's a distributed job scheduler. You submit a job over REST — run
it now, at a future time, or on a cron schedule — and it persists to Postgres and executes
when due, with retries and a dead-letter state. The interesting part isn't the API, it's that
several identical instances of the service run at once, all polling the same table, and a job
must execute exactly once across the whole fleet. I solved that without adding a broker or a
coordination service: the database is both the queue and the lock manager. Workers claim jobs
with `SELECT ... FOR UPDATE SKIP LOCKED` inside a short transaction, which makes each row
claimable by exactly one instance, and execution happens after that transaction commits so a
slow job never holds a lock or a connection. On top of that there's exponential backoff with
jitter, a reaper that reclaims jobs whose worker died mid-execution, and idempotency keys
enforced by a unique index."*

**Do not** open with "it's a CRUD API for managing jobs." Lead with the correctness problem.

**Follow-ups they'll ask:** everything below.

---

### 🔴 Q1.2 "Why multiple instances at all? Wouldn't one be simpler?"

Because multiple instances is the *default state* of a deployed service, not a scaling
add-on. Three reasons: availability (one dies, service continues), throughput (one JVM's
thread pool is a hard ceiling), and deploys — a rolling deploy runs the old and new version
simultaneously by definition. A scheduler that's only correct with one instance is a
scheduler that breaks every time you ship.

⚪ *Bonus:* "You could take a distributed lock and elect a single active scheduler, and some
systems do. But then that one node is a throughput ceiling and a failover story I'd have to
build and test. `SKIP LOCKED` lets every instance work simultaneously with no leader, so
there's no failover path to get wrong."

---

### 🔴 Q1.3 "What goes wrong if you just SELECT the due jobs and then UPDATE them?"

Both instances run the `SELECT` and get the same rows, because a plain `SELECT` in Postgres
takes no locks and blocks nothing — that's MVCC: readers never block writers and writers
never block readers. Then both `UPDATE`. The second one waits a few microseconds for the
first's row lock and then succeeds. Nothing errors. Both instances execute the same job.

It's a **check-then-act race** — structurally identical to
`if (!map.containsKey(k)) map.put(k, v)` in concurrent Java. The check and the act aren't
atomic, so another actor slips between them.

The nastiest property is that **MVCC makes it silent**. No exception, no deadlock, no
timeout. You find out when a customer is charged twice.

---

### 🔴 Q1.4 "How does `SKIP LOCKED` fix it? Show me the SQL."

```sql
SELECT * FROM jobs
WHERE status = 'PENDING' AND next_run_at <= now()
ORDER BY next_run_at
LIMIT 20
FOR UPDATE SKIP LOCKED;
```

`FOR UPDATE` takes the same row-level exclusive lock an `UPDATE` would take, but without
writing — it means "I intend to modify these, hands off." That collapses check-and-act into
one atomic step: I can't read a row without also claiming it.

`SKIP LOCKED` changes the *default* behaviour on contention. Normally a second transaction
would block waiting for those locks. With `SKIP LOCKED` it steps over the locked rows as if
they weren't there. So A takes 1–20 and B takes 21–40, immediately, with no waiting.

⚪ It landed in Postgres 9.5 explicitly because people were building queues on tables. It's a
job-queue primitive, not a hack.

---

### 🟡 Q1.5 "`FOR UPDATE` on its own is already correct. Why add `SKIP LOCKED`?"

Correctness isn't the issue — throughput is. With plain `FOR UPDATE`, B blocks for the full
duration of A's transaction. When A commits, `READ COMMITTED` re-fetches the new row versions
and re-runs B's `WHERE` clause against them; the rows are now `RUNNING`, so they're filtered
out and B scans onward. B gets the right answer, but it spent A's whole transaction idle.

With ten instances, nine are queued behind one. `SKIP LOCKED` reaches the same answer with no
one waiting. **Same correctness, no convoy.**

---

### 🟡 Q1.6 "A holds 1–20. B runs the same query with `LIMIT 20`. What does B get?"

Rows **21–40**, not zero and not an empty set. Skipped rows don't consume the limit. In the
plan, `LockRows` sits *below* `Limit`, so rows that fail to lock are never emitted upward and
Postgres keeps scanning until 20 rows are actually locked. You can see this in
`EXPLAIN (ANALYZE)`.

---

### 🟡 Q1.7 "Wouldn't `SERIALIZABLE` or `REPEATABLE READ` be safer?"

No — it would be actively worse here, and I tested this. Under `REPEATABLE READ`, if a row
you're about to lock changed after your snapshot was taken, Postgres aborts the transaction
with `could not serialize access due to concurrent update`. `SKIP LOCKED` doesn't rescue you,
because it only skips rows *currently locked* — not rows that were already updated and
committed. So a stronger isolation level turns contention into errors and retry storms.

`READ COMMITTED` plus an explicit row lock is both correct and non-blocking. **I chose the
lock, not the isolation level, because the lock is the precise tool.** Stronger isolation
isn't a free safety upgrade.

---

### 🔴 Q1.8 "Why database polling instead of Kafka / RabbitMQ / SQS?"

A scheduled job is **mutable, queryable state with a due time**. That's a database row, not a
log entry. Brokers give you delivery; they don't give you "run at 09:00 Tuesday," "cancel it
before it runs," "why did attempt 2 fail," or "increment the retry count in place."

The decisive point: even with Kafka I'd **still need the database** for status queries and
history. So the real comparison isn't Postgres vs Kafka, it's Postgres vs Postgres-plus-Kafka
plus keeping two systems consistent. One system that satisfies every requirement beat two
systems where one satisfies a sixth of them.

---

### 🔴 Q1.9 "Where does this design break, and what breaks first?"

Three ceilings, in the order I'd hit them:

1. **Latency floor.** A 5-second poll interval means ~2.5s average dispatch delay. If the
   product needed sub-100ms, polling is the wrong shape — I'd add `LISTEN/NOTIFY` to wake
   workers on insert.
2. **Write amplification.** Every job costs several row updates plus an execution row. MVCC
   means each `UPDATE` writes a new row version and leaves a dead one behind. A high-churn
   queue table is autovacuum's worst case.
3. **Poll contention.** More instances polling more often means more concurrent lock attempts
   on the same hot rows.

Realistically it holds to the **low thousands of jobs per second** on one tuned instance, and
the thing that breaks first is **autovacuum keeping up with dead tuples — not query latency**.
The claim query itself stays a sub-millisecond index scan.

Cheaper fixes before reaching for a broker: partition by status or job type, separate API
instances from worker instances, shorten the tail with `LISTEN/NOTIFY`. I'd switch to a broker
when the workload stops being *scheduled tasks* and becomes *high-volume streaming*, because
that's a genuinely different problem.

---

### ⚪ Q1.10 "What does MVCC actually do, mechanically?"

Postgres never updates a row in place. An `UPDATE` writes a new version and marks the old one
expired. Each version carries hidden `xmin` (creating transaction) and `xmax` (superseding
transaction) columns. Every statement runs against a snapshot of which transactions had
committed when it started, and a version is visible if its `xmin` committed before the
snapshot and its `xmax` didn't.

Consequences: readers never block writers, writers never block readers, writers *do* block
writers on the same row, and dead versions accumulate until autovacuum reclaims them — which
is why point 2 above is the real scaling limit.

---

### ⚪ Q1.11 "What are the row lock modes?"

Weakest to strongest: `FOR KEY SHARE` (what a foreign-key check takes), `FOR SHARE`
(read-and-don't-change), `FOR NO KEY UPDATE` (what a plain non-key `UPDATE` takes), and
`FOR UPDATE` (strongest). We use `FOR UPDATE`. All of them are held until `COMMIT` or
`ROLLBACK` — there is no unlock statement, which is precisely why the claim transaction has
to be short.

---

### 🟡 Q1.12 "What if a worker crashes right after claiming?"

Two different cases, and distinguishing them is the point:

- **Crash before the claim transaction commits** — the transaction aborts, the row locks are
  released, the row is still `PENDING`. Self-healing, nothing lost.
- **Crash after the claim commits but before the job finishes** — the row sits `RUNNING`
  forever, locked by an instance that no longer exists. Nothing in the database fixes this,
  because from Postgres's perspective the transaction committed successfully.

The second case is what the stale-job reaper solves *(Day 9)* — the same reasoning as an SQS
visibility timeout.

---

### 🔴 Q1.13 "Is `SKIP LOCKED` on its own enough to guarantee a job runs once?"

**No** — and this is the part people get wrong. Row locks live only as long as the
transaction holding them. If my claim transaction did nothing but
`SELECT ... FOR UPDATE SKIP LOCKED` and then committed, the very next poll — by the same
instance or another one — would get **the same rows back**, because they'd still be `PENDING`
and still due. I demonstrated this: two sequential claim transactions both returned rows 1,2,3.

You need two things working together:

- **`SKIP LOCKED` gives mutual exclusion** — no two workers claim the same row *concurrently*.
- **Writing `status='RUNNING'` inside that same transaction gives the claim durability** — the
  row stops matching the poll predicate, so it can't be claimed *again later*.

That's why the claim transaction is `SELECT ... FOR UPDATE SKIP LOCKED` **plus** the update to
`RUNNING`/`locked_by`/`locked_at`, committed atomically. The lock handles the race; the status
write handles the repeat.

⚪ *Bonus:* "You can express the whole thing as one statement —
`UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING *` — which is atomic by
construction. I kept it as an explicit transaction in JPA for readability and because I also
need `locked_by` and `locked_at` set from application state."

---

### 🟡 Q1.14 "Tell me about a bug you found." *(Amazon: Dive Deep)*

The one worth telling is Q1.13. The naive fix for duplicate execution is "add `SKIP LOCKED`,"
and it *looks* like it works — in a single-shot test the two workers get disjoint batches, so
you tick the box. The failure only appears once the poll loop runs a second time: the first
worker's transaction has committed and released its locks, and if the claim didn't also write
the status, the same rows are handed straight back out.

What I took from it: **the lock and the state change solve different halves of the problem, and
a test that only exercises the concurrent case will pass while the sequential case is broken.**
It's also why my concurrency test seeds a table and asserts on *total claims across repeated
polls*, not just that two simultaneous batches don't overlap.

---

## Section 2 — Data model and indexes (Day 2)
_to be filled in_

## Section 3 — Persistence and JPA (Day 3)
_to be filled in_

## Section 4 — API design (Day 4)
_to be filled in_

## Section 5 — Handlers and thread pools (Day 5)
_to be filled in_

## Section 6 — The claim transaction (Day 6)
_to be filled in_

## Section 7 — Retries and backoff (Day 8)
_to be filled in_

## Section 8 — Crash recovery (Day 9)
_to be filled in_

## Section 9 — Idempotency and delivery semantics (Day 10)
_to be filled in_

## Section 10 — Testing (Day 11)
_to be filled in_
