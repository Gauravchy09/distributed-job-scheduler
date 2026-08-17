# Day 2 Prep — Homework

Tomorrow is the **data model**: the `jobs` and `job_executions` tables, Flyway migrations, and
proving a partial index with `EXPLAIN ANALYZE` on 100k rows.

Concept-first rule applies: **you design the schema before you see mine.** Being wrong here is
free and useful. Seeing my answer first costs you the ability to ever find out what you'd have
chosen on your own.

Budget: ~45 minutes. Answer inline, under each prompt.

---

## Part A — Design the `jobs` table yourself

No SQL syntax marking, no rush. Write the `CREATE TABLE` as best you can and defend it.

Constraints to satisfy — the system must be able to:

1. Accept a job that runs **now**, at a **future timestamp**, or on a **cron schedule**
2. Store **arbitrary per-job input data**, different shape for every job type
3. Be claimed by a worker with `SKIP LOCKED`, recording **which** worker and **when**
4. Retry a failed job with exponential backoff, up to a **max attempt count**
5. Land in a **dead-letter** state when retries are exhausted
6. Reject a **duplicate submission** — same logical job submitted twice must not run twice
7. Answer "what is the status of job X" over REST
8. Be reclaimed if the worker holding it **dies mid-execution**

Your table:

```sql
CREATE TABLE jobs (
  -- your answer
);
```

**Now defend three specific choices:**

**A1.** What type is the primary key — `bigserial`, `uuid`, or something else? What breaks with
the one you didn't pick? *(Hint: think about who generates the id, and when the client needs to
know it.)*

**A2.** Requirement 8 says "reclaimed if the worker dies." Which **columns** did you add to make
that possible? A reaper has to run some `WHERE` clause to find those jobs — write it.

**A3.** Requirement 6, duplicate submission. What did you add? Is it a column, a constraint, an
index, or all three?

---

## Part B — Three type decisions

**B1. `TIMESTAMP` vs `TIMESTAMPTZ` for `next_run_at`.**

Your workers run in three regions. A job is scheduled for "09:00". Walk through what happens
under each type. Which do you pick and why? *(Second question: does `TIMESTAMPTZ` actually store
a timezone? Find out — the answer surprises most people.)*

**B2. `JSONB` vs typed columns for the job payload.**

You need to store per-job input. Options:
- a `JSONB` column
- a `TEXT` column holding serialised JSON
- a separate `job_parameters` key/value table

Pick one. Then argue the **strongest case against your own pick** — what do you lose?

**B3. Money.** Say a job payload includes an amount. Why is `float`/`double` wrong, and what do
you use instead? *(Not scheduler-specific — it's asked in interviews constantly and people get
it wrong.)*

---

## Part C — The state machine

List every state a job can be in. Then draw the legal transitions — an arrow per transition,
labelled with **what causes it**.

```
PENDING ──?──> ...
```

**C1.** Which transitions are made by the **worker**, and which by the **reaper**?

**C2.** Is `RUNNING → PENDING` legal? Who does it, and why is it dangerous if the wrong actor
does it? *(This is the trickiest question on this sheet. Think about the worker that isn't
actually dead — just slow.)*

**C3.** Is there a state you can enter but never leave? Should there be?

---

## Part D — Indexes, before we measure

The poll query, every 5 seconds, from every instance:

```sql
WHERE status = 'PENDING' AND next_run_at <= now() ORDER BY next_run_at
```

The table has **10 million** rows. **95%** are `SUCCEEDED`. About **200** are claimable.

**D1.** Write the index you'd create.

**D2.** Why is `CREATE INDEX ON jobs (status)` a bad answer? Two reasons — one about
selectivity, one about the size of the index itself.

**D3.** What is a **partial index**, and why is this workload close to the perfect case for one?

**D4.** Every index makes writes slower. This table is claim-heavy — every job is `UPDATE`d
several times. So: **what is the cost of the index you proposed in D1**, and why is it worth
paying? *(Tie this back to MVCC and dead tuples from Day 1.)*

We'll settle D1–D4 empirically tomorrow with `EXPLAIN ANALYZE` on real data. Commit to an
answer now so you have something to be right or wrong about.

---

## Part E — Still outstanding from today

- **Q1 redo.** The project in your own words — mechanism, not adjectives. Reconstruct it, don't
  recite it.
- **Q3, second half.** You ran Exercise 4. `SKIP LOCKED` and deadlocks — one sentence, why is it
  structurally impossible?
- **Q4.** 50M rows, 200 due, `Seq Scan` in the plan. Problem or not? Why is a plain B-tree on
  `status` the wrong fix?
- **Q5.** T1 holds `FOR UPDATE` on row 5. T2 runs `SELECT * WHERE id=5` with **no** `FOR UPDATE`.
  Blocks? Sees what? Name the mechanism.
- **Q6.** *"`SERIALIZABLE` guarantees correctness, so choosing `READ COMMITTED` is a correctness
  compromise for speed."* Rebut. Not with "it's faster."
- **Q7.** `LockRows` below `Limit` — why does that matter operationally? What breaks if reversed?
- **Q8.** Colleague proposes: 10 instances, each claims only `id % 10 = <instance_number>`, no
  locks. What's wrong with it? At least two things.

---

## Part F — One thing to look up

Read the Flyway docs on **migration naming and checksums** — 10 minutes, no more:
https://documentation.red-gate.com/fd/migrations-271585107.html

Come back able to answer: **why can you never edit a migration file that has already been
applied?** What does Flyway actually do to stop you, and what is the correct fix when a
deployed migration turns out to be wrong?
