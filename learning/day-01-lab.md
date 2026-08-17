# Day 1 — Practical Test Lab

**Format: predict, then run.** For every exercise, write your prediction down *before* you
execute. A prediction you got wrong teaches you more than five you got right — that gap is
exactly where your mental model is broken.

Answers are at the bottom. Do not read them first. You will only be lying to yourself.

---

## Setup

One terminal to start:

```powershell
docker compose exec postgres psql -U scheduler -d scheduler
```

```sql
DROP TABLE IF EXISTS claim_lab;

CREATE TABLE claim_lab (
  id          int PRIMARY KEY,
  status      text NOT NULL,
  next_run_at timestamptz NOT NULL,
  locked_by   text,
  attempts    int NOT NULL DEFAULT 0
);

INSERT INTO claim_lab (id, status, next_run_at) VALUES
  (1,  'PENDING',   now() - interval '10 min'),
  (2,  'PENDING',   now() - interval '9 min'),
  (3,  'PENDING',   now() - interval '8 min'),
  (4,  'PENDING',   now() - interval '7 min'),
  (5,  'PENDING',   now() - interval '6 min'),
  (6,  'PENDING',   now() - interval '5 min'),
  (7,  'RUNNING',   now() - interval '4 min'),
  (8,  'FAILED',    now() - interval '3 min'),
  (9,  'PENDING',   now() + interval '1 hour'),
  (10, 'PENDING',   now() + interval '2 hour'),
  (11, 'SUCCEEDED', now() - interval '1 min'),
  (12, 'PENDING',   now() - interval '30 sec');
```

Keep this handy — it resets the table between exercises:

```sql
UPDATE claim_lab SET status='PENDING', locked_by=NULL WHERE id IN (1,2,3,4,5,6,12);
```

**Before you run anything: how many rows are due and claimable?** Write the number down.
10 rows
Open **three** psql terminals. Call them T1, T2, T3.

---

## Exercise 1 — Read the predicate

T1:
```sql
BEGIN;
SELECT id FROM claim_lab
WHERE status = 'PENDING' AND next_run_at <= now()
ORDER BY next_run_at
LIMIT 3
FOR UPDATE SKIP LOCKED;
-- leave the transaction OPEN
```

**Predict:** which ids?
1,2,3

T2, same query:

**Predict:** which ids?

T3, same query:

**Predict:** which ids? How many rows come back? Why that number?

Now `ROLLBACK;` in all three.

> The T3 answer is the one that matters. If you predicted 3 rows, your model of `LIMIT`
> is wrong in a way that will cost you in an interview.

---

## Exercise 2 — The lock strength trap

Reset the table first.

T1:
```sql
BEGIN;
SELECT id FROM claim_lab WHERE id = 1 FOR SHARE;
-- leave OPEN
```

`FOR SHARE` is a *shared* lock — "I'm reading this, don't change it." It is weaker than
`FOR UPDATE`, and multiple transactions can hold it on the same row simultaneously.

T2:
```sql
BEGIN;
SELECT id FROM claim_lab
WHERE status = 'PENDING' AND next_run_at <= now()
ORDER BY next_run_at
LIMIT 3
FOR UPDATE SKIP LOCKED;
```

**Predict:** does T2 get row 1, or skip it?

Then reset, and try the reverse: T1 takes `FOR SHARE` on row 1, and T2 also asks for
`FOR SHARE` on row 1 (not `FOR UPDATE`). **Predict:** blocked, skipped, or granted?

`ROLLBACK;` everywhere.

---

## Exercise 3 — Where does Postgres keep the row lock?

Reset. T1:
```sql
BEGIN;
SELECT id FROM claim_lab WHERE id IN (1,2,3) FOR UPDATE;
-- leave OPEN
```

T2 — go hunting for those three row locks:
```sql
SELECT locktype, relation::regclass AS rel, page, tuple, mode, granted
FROM pg_locks
WHERE relation = 'claim_lab'::regclass;
```

**Predict:** how many rows does `pg_locks` show for the three locked tuples?

Now look at what it *does* show:
```sql
SELECT locktype, transactionid, mode, granted
FROM pg_locks
WHERE pid = (SELECT pid FROM pg_stat_activity WHERE query LIKE '%FOR UPDATE%' AND state='idle in transaction' LIMIT 1);
```

**The question to answer:** if the three row locks aren't in `pg_locks`, where physically are
they stored? And what does that imply about how many rows you can lock at once?

`ROLLBACK;`

---

## Exercise 4 — Can two claim transactions deadlock?

Reset. First, prove deadlock is *possible* with plain `FOR UPDATE`.

T1:
```sql
BEGIN;
SELECT id FROM claim_lab WHERE id = 1 FOR UPDATE;
```

T2:
```sql
BEGIN;
SELECT id FROM claim_lab WHERE id = 2 FOR UPDATE;
```

Now T1:
```sql
SELECT id FROM claim_lab WHERE id = 2 FOR UPDATE;   -- blocks
```

Then T2:
```sql
SELECT id FROM claim_lab WHERE id = 1 FOR UPDATE;   -- ???
```

**Predict** what happens to T2, and roughly how long it takes. (Our `docker-compose.yml` sets
`deadlock_timeout=1s` — that's why.)

Also check the server log afterwards, since we turned on `log_lock_waits`:
```powershell
docker compose logs postgres --tail 40
```

Now `ROLLBACK` both, reset, and **repeat the exact same sequence with `FOR UPDATE SKIP LOCKED`**
on every statement.

**Predict:** does it deadlock this time?

> This is the strongest single argument for `SKIP LOCKED` that most people never think of.
> Make sure you can say *why* in one sentence.

---

## Exercise 5 — Diagnose four claim implementations

No running required yet. Read each, decide **correct or broken**, and if broken say *exactly*
what goes wrong and *when* it shows up. Then run the ones you're unsure about.

**Variant A**
```sql
BEGIN;
UPDATE claim_lab SET status='RUNNING'
WHERE id IN (
  SELECT id FROM claim_lab
  WHERE status='PENDING' AND next_run_at <= now()
  ORDER BY next_run_at LIMIT 3
)
RETURNING id;
COMMIT;
```

**Variant B**
```sql
BEGIN;
UPDATE claim_lab SET status='RUNNING'
WHERE id IN (
  SELECT id FROM claim_lab
  WHERE status='PENDING' AND next_run_at <= now()
  ORDER BY next_run_at LIMIT 3
  FOR UPDATE SKIP LOCKED
)
RETURNING id;
COMMIT;
```

**Variant C**
```sql
BEGIN;
SELECT id FROM claim_lab
WHERE status='PENDING' AND next_run_at <= now()
ORDER BY next_run_at LIMIT 3
FOR UPDATE SKIP LOCKED;
COMMIT;
```

**Variant D**
```sql
BEGIN;
SELECT id FROM claim_lab
WHERE status='PENDING' AND next_run_at <= now()
ORDER BY next_run_at LIMIT 3
FOR UPDATE SKIP LOCKED;
COMMIT;

-- application reads the ids, then:
BEGIN;
UPDATE claim_lab SET status='RUNNING' WHERE id IN (1,2,3);
COMMIT;
```

**To actually prove Variant A is broken**, run it in T1 and T2 *concurrently* — paste the
`BEGIN;` + `UPDATE` into both (without `COMMIT`), then commit both. Compare the two
`RETURNING` outputs.

---

## Exercise 6 — Write it yourself

This is one of the four things you write with no help from me *(spec §12)*. Attempt it now,
before Day 6, so you find out what you don't understand yet.

Write **one SQL statement** that:

1. claims up to 5 due jobs,
2. is safe when 10 workers run it simultaneously — no duplicates, no blocking,
3. sets `status='RUNNING'`, `locked_by='worker-1'`, and increments `attempts`,
4. returns the claimed rows so the caller knows what to execute,
5. is fair — oldest due job first.

Then answer: **which of those five requirements is the one that stops it being a plain
`UPDATE ... WHERE status='PENDING' LIMIT 5`?** (Trick: `UPDATE` doesn't take `LIMIT` in
Postgres. Why not, do you think?)

---

## Exercise 7 — The one that isn't about SQL

Your claim transaction commits, and then your worker executes the job — an HTTP call that
takes 30 seconds.

**Q:** Why must the execution happen *after* the claim transaction commits, rather than inside
it? Give **two** independent reasons — one about locks, one about a resource that has nothing
to do with Postgres locking.

**Q:** And what new problem did you just create by moving execution outside the transaction?
Which day of the plan solves it?

---
---
---

# ANSWERS

Stop. Have you run all seven?

---

**Setup.** 7 rows are due and claimable: **1,2,3,4,5,6,12**. Row 7 is `RUNNING`, 8 is `FAILED`,
11 is `SUCCEEDED` — wrong status. Rows 9 and 10 are `PENDING` but scheduled in the future.
Being `PENDING` isn't enough; being due isn't enough. Both predicates matter.

**Ex 1.** T1 → `1,2,3`. T2 → `4,5,6`. T3 → **`12` only — one row.** There are only 7 claimable
rows and six are locked. `LIMIT 3` is a ceiling, not a quota; Postgres scans until it has
locked 3 rows *or* runs out of rows. It does not block waiting for more to appear. An empty or
short batch is the normal, expected signal that the queue is drained.

**Ex 2.** T2 **skips row 1** and returns `2,3,4`. `FOR UPDATE` conflicts with `FOR SHARE` — a
shared lock still blocks an exclusive one, so from `SKIP LOCKED`'s point of view the row is
locked and gets stepped over. The lesson: `SKIP LOCKED` skips rows locked in any *conflicting*
mode, not just rows locked `FOR UPDATE`.

The reverse case — two transactions both taking `FOR SHARE` on row 1 — is **granted
immediately**. Shared locks don't conflict with each other. That's the whole point of a
lock *mode* hierarchy: conflict is a property of the pair, not of the row.

**Ex 3.** `pg_locks` shows **zero** tuple locks. It shows a `relation` lock in
`RowShareMode` on the table, and a `transactionid` lock in `ExclusiveMode` that T1 holds on
its own transaction id — but the three row locks are not there.

They're stored **in the tuples themselves, on the heap page** — Postgres writes the locking
transaction's id into the row version's `xmax` header field, with infomask bits recording the
lock mode. Consequences worth saying out loud in an interview:

- Row locks cost **no shared memory**, so there is **no lock escalation** in Postgres. You can
  lock a million rows in one transaction and nothing overflows a lock table. SQL Server and
  DB2 escalate row locks to page/table locks under memory pressure — Postgres never has to.
- The price is that locking a row **dirties the page** and generates WAL, so even a
  "read-only" `SELECT ... FOR UPDATE` causes disk writes. Your queue table is write-heavy even
  where it looks like it's only reading.
- `pg_locks` only gains a `tuple` row *transiently*, while a transaction is queueing to wait on
  a contended row. That's a waiter, not the holder.

**Ex 4.** With plain `FOR UPDATE`: **T2 is aborted** with
`ERROR: deadlock detected` after about 1 second (`deadlock_timeout`). T1 then proceeds. Note
that Postgres does not *prevent* deadlocks — it lets them form, sleeps for `deadlock_timeout`,
then runs a cycle-detection pass on the wait-for graph and kills the cheapest victim.

With `SKIP LOCKED`: **no deadlock, ever.** The one-sentence reason:

> **A deadlock requires a cycle in the wait-for graph, and `SKIP LOCKED` transactions never
> wait — so no edge is ever added to that graph.**

T1's second statement simply returns zero rows instead of blocking. This is a genuine
correctness-under-load property, not just a throughput optimisation: a claim loop built on
plain `FOR UPDATE` can deadlock and abort transactions under contention, and the fix is either
strict global ordering of row access or `SKIP LOCKED`. We get it for free.

**Ex 5.**

- **A — broken, and silently.** The inner `SELECT` takes no locks, so both workers read
  `1,2,3` against their own snapshots. Both then try to `UPDATE` those rows. Worker B blocks on
  the row locks, and when A commits, `READ COMMITTED` re-evaluates B's `WHERE` clause — but
  B's outer condition is `id IN (1,2,3)`, and the ids obviously still match. B updates the same
  rows and `RETURNING` hands it `1,2,3`. **Both workers execute the same three jobs, with no
  error.** This is Q1.3's check-then-act race, and the `RETURNING` clause makes it look
  authoritative.
- **B — correct.** Atomic by construction: the lock and the state change are in one statement.
- **C — broken.** This is the Lab 3b bug. The lock dies at `COMMIT` and nothing changed, so the
  next poll returns the same rows. It passes a concurrent test and fails on the second loop
  iteration.
- **D — broken.** The gap between the two transactions is unprotected. The first `COMMIT`
  releases every lock; another worker can claim `1,2,3` in that window, and the second
  transaction then stamps `RUNNING` over rows someone else already owns. Worse than C, because
  it *looks* like it does the update. **The lock and the write must be in the same
  transaction** — that's the invariant.

**Ex 6.** No answer given, deliberately — this is yours to write. Check it against these
requirements, then show me:

- Does it hold the lock and write the status in **one** transaction?
- Does it use `SKIP LOCKED`, not plain `FOR UPDATE`?
- Does it `ORDER BY next_run_at` *inside the subquery*, where it affects which rows get locked?
- Does it `RETURNING` the columns the worker actually needs, not just the id?

On `UPDATE ... LIMIT`: Postgres deliberately doesn't support it, because `UPDATE` has no
defined row order — "update 5 rows" without `ORDER BY` is non-deterministic, and `UPDATE`
can't take `ORDER BY` either. The subquery is not a workaround; it's the mechanism that makes
"which 5?" a well-defined question.

**Ex 7.** Two reasons execution must happen after the commit:

1. **Locks are held until commit** — there is no unlock statement. A 30-second HTTP call inside
   the transaction means a 30-second row lock, and a growing table of rows no one else can even
   *see* as skippable without paying scan cost.
2. **Connection pool exhaustion** — this one has nothing to do with locking. An open
   transaction pins a JDBC connection for its whole life. With a HikariCP pool of 10 and
   30-second jobs, ten concurrent jobs starve every other part of the application, including
   the REST API. This is usually what actually takes the service down first.

⚪ A third: long transactions hold back `xmin`, which **blocks autovacuum from cleaning dead
tuples anywhere in the database** — not just this table.

The new problem you created: **the claim is now committed but the execution is not
transactional**. If the worker dies mid-job, the row is stuck in `RUNNING` forever, owned by a
process that no longer exists — and Postgres sees nothing wrong, because that transaction
committed successfully. That's the **stale job reaper, Day 9**, and it's the same reasoning as
an SQS visibility timeout.
