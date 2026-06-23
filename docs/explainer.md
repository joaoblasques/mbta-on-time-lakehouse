# The project in plain English

A friendly map of what this project is, what we've built, what we just did, and what's next — no
jargon. If you ever feel lost in the steps, start here.

---

## What is this project? (the one-paragraph version)

We're answering a simple question — **"Is the MBTA train/bus late, and where?"** — with a real,
automated data system. Every couple of minutes a robot grabs the MBTA's live position data,
stores it, compares it to the timetable, and works out how late things are. The results show up
as an **on-time-performance (OTP)** scoreboard. The whole thing runs by itself in the cloud, fixes
small problems on its own, and even writes up its own findings. It's a portfolio piece to show I
can build a modern **data lakehouse** on Databricks + Google Cloud.

---

## The big picture (a kitchen analogy)

Think of it like a restaurant kitchen turning raw ingredients into a finished dish:

```
MBTA live data  →  GCS (the pantry)  →  Volume (the fridge inside Databricks)
                                              │
                                   Bronze = raw ingredients (decoded data)
                                              ▼
                                   Silver = prepped (how late is each stop?)
                                              ▼
                                   Gold   = the plated dish (the OTP scoreboard)
                                              ▼
                                   Dashboard + the "Dreamer" that writes insights
```

- **Bronze / Silver / Gold** is the standard "medallion" layout: raw → cleaned → business-ready.
- Each arrow is automated on a schedule. Nobody presses a button.

---

## The cast (what each moving part does)

| Piece | Plain-English job |
|---|---|
| **Poller** | Every 2 min, grabs MBTA's live feed and drops it in cloud storage (GCS). |
| **Copier** | Every 15 min, moves new files from GCS into Databricks. |
| **Medallion job** | Every hour, turns raw data → lateness → the OTP scoreboard. |
| **Dreamer** | Each night, reads the scoreboard, writes a plain-English insight, and opens a pull request with it. |
| **Monitor** | Every 30 min, checks the hourly job; if it failed, retries it, or files a ticket. |

The last two are the "self-managing" brain: the system watches and improves *itself*.

---

## What we've built so far (the journey, simply)

1. **Got data flowing** — live MBTA feed → cloud → Databricks, end to end.
2. **Computed the answer** — lateness per stop, then the OTP scoreboard, with a dashboard.
3. **Made it trustworthy** — automatic data-quality checks + tests, so bad data can't sneak through.
4. **Made it run itself** — schedules for every step; nothing is manual.
5. **Gave it a brain (agentic layer)** — the *Dreamer* writes nightly insights, the *Monitor*
   heals failures. It proposes changes as pull requests for me to approve.
6. **Made it reproducible** — all the cloud setup is code (Terraform), and the Databricks job is
   code too (**Asset Bundles**). CI checks every change; it even deploys infra with no passwords
   stored (keyless **Workload Identity Federation**).
7. **Made the logic testable** — the tricky math lives in a small tested library (`src/transforms`).

---

## What we JUST did: the "transforms wheel" (in simple terms)

**The concept.** The hardest, easiest-to-get-wrong math — *"how late is this, accounting for
timezones and after-midnight buses?"* — used to be **copy-pasted** inside the notebooks. Copy-paste
means two copies that can silently disagree (one gets fixed, the other doesn't). That's "drift."

**What we wanted.** *One* copy of the logic, **tested**, and have the notebooks **use that exact
copy** — so the code we test is literally the code that runs.

**The steps.**
1. Move the math into one small library (`src/transforms`).
2. Write tests that prove it's correct (including the nasty after-midnight case).
3. Package that library as a **wheel** (a wheel is just Python's word for a shippable bundle of code).
4. Tell the Databricks job to install that wheel, and change the notebooks to *import* it instead
   of holding their own copy.

**The development steps (what I actually typed).**
- Added a build recipe to `pyproject.toml` (so `src/transforms` can become a wheel), keeping the
  wheel dependency-free so it installs fast.
- Configured the **Asset Bundle** to build the wheel and attach it to the job.
- Rewrote notebooks `04`/`05` to `from transforms... import ...`.
- **Tested it safely first** in a "dev" copy of the job, *then* promoted to the live "prod" job.

**The result.** No more drift — there's one tested source of truth, and it's what runs. ✅

---

## What we did: streaming — now LIVE ✅ (in simple terms)

*(This was "next" — it's now shipped. Verified at cutover: 32.6M raw rows across 1,657 files,
system OTP 59.6%.)*

**The problem before.** Every hour, the system re-read a **chunk of recent files** to stay fast.
It worked, but it was a bit wasteful and only kept a rolling window of history.

**The concept of streaming.** Instead of "re-read a chunk every time," we want **"only handle what's
*new* since last time."** Like a mail sorter who only opens *today's* mail, not the whole mailbox
again. That's **incremental** processing — and it's how grown-up data systems scale.

**The catch (and our approach).** True streaming usually needs a computer that's always on, which
our free Databricks tier doesn't give us. So we use a clever middle ground: a streaming engine
that **wakes up on schedule, processes only the new files, and goes back to sleep** (Databricks
calls this *Auto Loader* + *Trigger.AvailableNow*). Same "only new stuff" benefit, no always-on cost.

**The steps.**
1. Switch the raw (bronze) step to "only read new files" using a **checkpoint** (a bookmark that
   remembers what's already been processed).
2. Keep the full history of raw data (we no longer have to throw old data away).
3. Have the "prepped" (silver) step look at just the **recent days** so it stays fast.

**The development steps (the plan).**
- Build it in a **separate "dev" copy first**, writing to test tables (so the live system is never
  at risk).
- Run it **twice** to prove the bookmark works: the second run should pick up *only* new files.
- Check the OTP numbers match the current system.
- Only then **switch the live system over** in one reviewed change, with the Monitor watching.

**Why bother (the payoff).** It scales cleanly, keeps all history, and demonstrates real
**Structured Streaming** — a headline data-engineering skill — done within free-tier limits.

> Full technical version: `docs/streaming.md`. Architecture decisions: `docs/architecture.md`.
