Distributed NL-to-Regex Data Processing Platform

This is a web application that allows users to:
- upload CSV or Excel files,
- describe text patterns in natural language, and
- apply transformations at scale.

Natural language is converted to regex via an LLM, cached in Redis, and applied across millions of rows using PySpark asynchronously, without ever blocking the web request cycle.

**Live app:** http://3.106.244.151:5173

### Contents
- [Demo](#demo)
- [Setup & Run Instructions](#setup--run-instructions)
- [Architecture Overview](#architecture-overview)
- [Why This Architecture](#why-this-architecture)
- [Transformations — Strategy/Registry Pattern](#transformations--strategyregistry-pattern)
- [PySpark Partitioning & Parallelism Justification](#pyspark-partitioning--parallelism-justification)
- [Known Trade-offs & Limitations](#known-trade-offs--limitations)
- [Tech Stack](#tech-stack)

---

## Demo

### Transformations

#### Find & Replace
https://github.com/user-attachments/assets/39800371-2d0d-4d09-9efd-ef8ab216d75f

1. The column dropdown populates from the file's actual headers as soon as it's selected.
2. On submit, the API returns a job ID immediately.
3. A Celery worker picks the task off a Redis queue, sends the prompt to an LLM, validates the generated regex for safety, and applies it as a native Spark transformation.
4. Results are paginated straight from the Parquet output.

#### Extract into a New Column
https://github.com/user-attachments/assets/697368b4-b60c-416e-a049-4a6a014041bf

1. A second transformation type: extracting a pattern into a brand-new column.
2. Same async pipeline underneath, different Spark operation — the platform uses a strategy pattern so each transformation type (find & replace, extract, format-standardization) plugs into the same job/worker machinery.

#### Standardize Format
https://github.com/user-attachments/assets/586b37d7-f4c2-4a72-a7f3-47aa8192b671

1. A third type maps natural language to a constrained set of formatting operations (e.g. title-casing a name column), where the LLM chooses from a safe allow-list rather than generating free-form output.

### Scale & Performance

#### Million-Row Job
https://github.com/user-attachments/assets/f8fda56d-b446-450c-b5eb-1a97ed1db9ac

1. The same operation against a one-million-row file.
2. Flower (the worker monitoring dashboard) shows the task running live on the worker with its arguments and state.
3. This uploaded file is processed in ~6 seconds.
4. For repeated operations, the worker logs show zero LLM calls because the pattern was already cached in Redis from the earlier job, keyed on the prompt.
5. Identical requests never hit the LLM twice, so cost and latency stay flat regardless of file size.

**Hard numbers behind that clip** — a generated 1,000,000-row CSV (~115MB: Name, Id, Phone, Email, Notes columns with randomized values), processed end-to-end on the deployed 2-vCPU instance:
- Wall time: 5.6 seconds from task pickup to SUCCESS — including file read, transformation, and Parquet write
- LLM calls: zero — the prompt had been cached from a prior job, demonstrating the cache working at scale
- Pagination: ~10,000 pages of 100 rows; page loads remain fast at arbitrary page offsets
- The upload itself is streamed to disk in chunks (`file.chunks()`) — the web process never holds the full file in memory

#### Paginated Results
https://github.com/user-attachments/assets/e4ab53a0-df75-492b-92b1-22f4681460b6

1. Ten thousand pages of results, served page-by-page from Parquet, showing that the browser never receives more than one page at a time, even at a million rows.

### Reliability & Error Handling

#### Failure Handling
https://github.com/user-attachments/assets/3faf4168-1674-4e37-8066-1b280c8af4a2

1. Error handling matters as much as the happy path.
2. This prompt matches no supported operation, so the LLM returns an explicit rejection token and the job fails immediately with a clear message (instead of retrying a request that could never succeed).

#### Cancellation
https://github.com/user-attachments/assets/e3c830d8-e3ec-4067-96f7-de4e290efb61

1. A running job can be cancelled cleanly.
2. The worker checks for cancellation between stages and stops without corrupting partial results.

---

## Setup & Run Instructions

### Prerequisites
    1. Docker with the Compose plugin
    2. A Google Gemini API key (free tier: https://ai.google.dev)

### Local development
    1. git clone https://github.com/zlib-eng/distributed-nl-to-regex.git
    2. cd distributed-nl-to-regex

### Create your environment file
    3. cp .env.example .env   # then fill in GEMINI_API_KEY and passwords

`.env` values required:

    POSTGRES_DB=rhombus
    POSTGRES_USER=rhombus
    POSTGRES_PASSWORD=<choose one>
    POSTGRES_HOST=db
    POSTGRES_PORT=5432
    REDIS_URL_BROKER=redis://redis:6379/0
    REDIS_URL_RESULTS=redis://redis:6379/1
    REDIS_URL_CACHE=redis://redis:6379/2

    SHARED_FILES_PATH=/shared/files

    GEMINI_API_KEY=<your key>

    DJANGO_SECRET_KEY=<generate: python3 -c "import secrets; print(secrets.token_urlsafe(50))">
    DJANGO_DEBUG=True
    DJANGO_ALLOWED_HOSTS=localhost,127.0.0.1

    FLOWER_USER=admin
    FLOWER_PASSWORD=<choose one>

Then:

    4. docker compose up --build -d
    5. docker compose exec web python manage.py migrate
    6. docker compose exec web python manage.py createsuperuser

App: http://localhost:5173

API: http://localhost:8000/api/

Admin: http://localhost:8000/admin/

Flower (worker monitoring): http://localhost:5555

### Production deployment
The production stack (`docker-compose.prod.yml`) differs from development in the following:
- Gunicorn replaces the Django dev server,
- the frontend is a pre-built static bundle (`npm run build`, with the API base URL baked in from `frontend/.env.production`),
- source-code volume mounts are removed (code is baked into images at build time), and
- `restart: unless-stopped` policies keep services alive across reboots.

        7. docker compose -f docker-compose.prod.yml up -d --build
        8. docker compose -f docker-compose.prod.yml exec web python manage.py migrate

Deployed on an AWS EC2 instance (2 vCPU / 4GB RAM — PySpark's JVM requirements rule out sub-1GB free hosting tiers). Ports 8000 (API) and 5173 (frontend) are open publicly; 5555 (Flower) is restricted to a single admin IP since it exposes internal task state.

---

## Architecture Overview

<img width="1695" height="1065" alt="System architecture diagram: the synchronous fast path (React → Django → Redis) and the asynchronous slow path (Celery worker → LLM/Spark → Parquet)" src="https://github.com/user-attachments/assets/75132c7a-bfcb-4425-b7d5-5c796427dc33" />

Two flows run concurrently.

The fast path is synchronous and takes milliseconds:
- React posts the file and job parameters,
- Django saves the file to the shared volume, persists a Job row, pushes a task message onto Redis, and returns a job ID immediately.

The slow path is asynchronous:
- a Celery worker pops the task,
- resolves the natural-language prompt to a transformation spec (checking the Redis cache before calling the LLM),
- applies the transformation via native PySpark functions, and
- writes Parquet output back to the shared volume while reporting progress to Redis throughout.

Meanwhile React polls the status endpoint every two seconds, then fetches paginated results once the job succeeds.

**Observability:** Flower provides real-time worker monitoring at port 5555 (basic-auth protected): live task state, per-task runtimes and arguments, success/failure/retry counts, and task history — this is what's shown running live in the [Million-Row Job demo](#scale--performance) above.

---

## Why This Architecture

### Why Celery instead of processing inline?
A million-row Spark job takes seconds to minutes; a web request must return in milliseconds. Any heavy work inside a Django view blocks a web worker thread — under load, all threads block and the server stops responding to everyone. The web process here does nothing but coordinate: validate, persist, dispatch, respond.

### Why Redis wears three hats?
Redis serves as the Celery message broker (db 0), the result backend for live progress state (db 1), and the LLM response cache (db 2). All three are access patterns Redis is built for: fast queue operations, high-frequency small reads for polling, and TTL-based key-value caching. PostgreSQL and Redis are not competitors here — PostgreSQL holds durable relational records (the Job row survives a restart), Redis holds fast ephemeral state (progress percentages, cached patterns). Losing Redis state is acceptable; losing job records is not.

### Why the LLM never sees the column selection.
The LLM's only responsibility is converting a natural-language description into a pattern — it receives the prompt and nothing else. Column selection is deterministic, chosen from a dropdown validated against the file's actual schema. This decoupling keeps the regex cache effective across different jobs and columns (the same prompt yields the same cached pattern regardless of which column it targets), isolates failure domains (an LLM outage cannot break column selection), and avoids a class of silent errors where the model guesses the wrong column from ambiguous phrasing.

### Why LLM calls are O(1) per job, never O(n) per row.
Every transformation resolves to exactly one LLM call (or zero, on a cache hit) regardless of whether the file has ten rows or ten million. The LLM interprets intent once; Spark applies the result at scale using native, vectorized functions. This is the constraint that keeps the design scale-safe — a per-row LLM classification feature, however appealing, would mean a million API calls per job and was deliberately excluded.

### Why polling instead of WebSockets.
WebSockets would deliver progress updates instantly instead of on a 2-second cadence — but require Django Channels, an ASGI server, and stateful per-client connections that complicate horizontal scaling. For jobs measured in seconds to minutes, a 2-second staleness window is imperceptible. Polling is the simplest correct solution for the latency requirement of the project.

### Why Parquet output.
Spark writes results as compressed, columnar Parquet. For the paginated read pattern (write once, read in small pages), Parquet is a dramatically better fit than inserting a million rows into PostgreSQL per job — which would create write load and storage bloat on exactly the wrong tool. Django's results endpoint reads the Parquet directory (handling Spark's multi-part-file output transparently via pandas/PyArrow) and serves 100-row pages.

---

## Transformations — Strategy/Registry Pattern

Three transformation types run through the same async/Spark pipeline:

|          Type         	|               LLM output              	|         Spark function         	|
|:---------------------:	|:-------------------------------------:	|:------------------------------:	|
| Find & Replace        	| free-form regex (validated)           	| regexp_replace                 	|
| Extract to New Column 	| free-form regex (validated)           	| regexp_extract → new column    	|
| Standardize Format    	| constrained choice from an allow-list 	| upper / lower / initcap / trim 	|

Each type is a class implementing a two-method interface (`generate_spec`, `apply`), registered in a dictionary. The Celery task retrieves the right strategy by name and never branches on type — adding a fourth transformation means one new class and one registry line, with zero changes to the task, views, or error handling. This is the Open/Closed principle applied deliberately: the earlier single-type implementation would have required editing tested task code for every addition.

The two regex-based types share one LLM path and one cache namespace (identical prompts yield identical patterns regardless of what's done with the match). Standardize Format demonstrates a deliberately different LLM integration pattern: instead of generating open-ended text that must be validated for syntax and safety, the model selects from a fixed enum — including an explicit `NONE` escape hatch for requests that match no supported operation, which fails the job immediately with a clear message rather than force-fitting a wrong-but-valid choice.

### Regex safety
Generated patterns are validated before touching data: syntax-checked via `re.compile`, and screened for nested-quantifier shapes (`(a+)+`) associated with catastrophic backtracking. Validation failures raise a dedicated exception class that fails the job immediately — deterministic failures (bad pattern, missing column) skip the retry cycle entirely, while transient failures (network errors, API rate limits) retry with exponential backoff (5s → 25s → 125s).

---

## PySpark Partitioning & Parallelism Justification

The Spark session runs in `local[*]` mode, using all available CPU cores on the worker machine. No manual partitioning is implemented, deliberately: `spark.read.csv` produces a sensible default partitioning for this workload, and every transformation is a native Spark SQL function (`regexp_replace`, `regexp_extract`, `initcap`, etc.) — vectorized operations that Spark distributes across partitions automatically. The critical design constraint was avoiding Python UDFs, which would force row-by-row execution through the Python interpreter and defeat the purpose of using Spark at all.

`spark.sql.shuffle.partitions` is reduced from the default 200 to 4, appropriate for a single-machine deployment. Scaling to a genuine multi-node cluster would require changing one configuration line (`master("local[*]")` → a cluster manager URL) — the transformation code itself is already cluster-ready because it uses only distributed-safe native functions.

---

## Known Trade-offs & Limitations

#### Page refresh loses frontend job state.
The job continues processing server-side, but the UI has no jobs-list view to re-attach to it. A localStorage-persisted job ID or a job history endpoint would resolve this.

#### Ambiguous prompts can yield valid-but-mismatched patterns.
Observed concretely in testing: "extract the area code" against Australian two-digit area codes produced a US-style three-digit pattern — syntactically valid, semantically wrong for the data, resulting in a correct-but-empty extraction. The system behaves properly (zero matches, no failure), but the user experience would benefit from a pre-execution match-count preview so interpretation mismatches surface before full processing. This is the most instructive limitation of NL-driven transformation generally: natural language underspecs, and the pipeline cannot know the user's data format unless told.

#### Spark's CSV reader strips leading/trailing whitespace at ingestion
by default, making the TRIM operation a no-op for CSV-sourced data (it remains useful for Excel sources). Pinning `ignoreLeadingWhiteSpace`/`ignoreTrailingWhiteSpace` explicitly would preserve raw whitespace if that mattered for a use case.

#### Files with partially blank headers
have those columns excluded from selection (pandas names them `Unnamed: N`; Spark names the same columns `_c N` — a cross-library inconsistency that previously caused a confusing runtime failure, now caught at upload with a user-facing warning). Spark's fallback names may still appear in results if such columns exist in the source.

#### Single worker with concurrency 2.
Each concurrent Spark task spawns a JVM; concurrency is capped to protect the 4GB instance. Horizontal scaling would add worker containers (the architecture already supports it — workers coordinate only through Redis).

#### `transformation_type` dispatch uses a registry dict.
At the current three types this is the right size; if the catalogue grew substantially, per-type serializer validation and UI configuration would also want to become part of each strategy class rather than living in the view.

---

## Tech Stack
<<<<<<< HEAD
Django + Django RestFramework · Celery · Redis · PySpark · PostgreSQL · React (Vite) · Google Gemini API · Flower · Docker Compose · Gunicorn
=======
Django + DRF · Celery · Redis · PySpark · PostgreSQL · React (Vite) · Google Gemini API · Flower · Docker Compose · Gunicorn


## Demo Video
[Embed the video here — see recording guide]

transform fail
https://youtu.be/96AqT8GLidY 

standardize format
https://youtu.be/N5a5D-XcjDY

paigination
https://youtu.be/95OHsUBYWKc 

million row
https://youtu.be/_-ze8hXNIJs

find replace 
https://youtu.be/gaz6mX4spVg

extract
https://youtu.be/oFDHKSTfOeo

cancelled
https://youtu.be/Eovjzsk5MvQ 
>>>>>>> 6e59f08 (added tests)
