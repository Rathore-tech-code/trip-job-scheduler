# Trip job scheduler: HLD and LLD

These diagrams describe the code in this repository. They show the local three-node deployment from `compose.yaml`; production networking and an external notification provider are outside this implementation.

## High-level design (HLD)

### System and deployment

```mermaid
flowchart LR
    user[Admin / operator / employee]
    browser[Browser dashboard<br/>HTML, CSS, JavaScript]
    apiClient[HTTP API client]
    metrics[Metrics reader]

    subgraph cluster[Three Spring Boot 3 nodes, Java 21]
        direction TB
        n1[Node 1 :8080<br/>REST API + security<br/>scheduler worker]
        n2[Node 2 :8081<br/>REST API + security<br/>scheduler worker]
        n3[Node 3 :8082<br/>REST API + security<br/>scheduler worker]
    end

    subgraph database[PostgreSQL 16: shared source of truth]
        direction TB
        config[(tenant, app_user, job)]
        coordination[(worker_node, tenant_slot)]
        execution[(job_run, effect)]
    end

    user --> browser
    browser -->|HTTP Basic + JSON| n1
    apiClient -->|HTTP Basic + JSON| n1
    apiClient -->|HTTP Basic + JSON| n2
    apiClient -->|HTTP Basic + JSON| n3
    metrics -->|/actuator/health or Prometheus| n1
    n1 <-->|JDBC transactions| config
    n1 <-->|JDBC transactions| coordination
    n1 <-->|JDBC transactions| execution
    n2 <-->|JDBC transactions| config
    n2 <-->|JDBC transactions| coordination
    n2 <-->|JDBC transactions| execution
    n3 <-->|JDBC transactions| config
    n3 <-->|JDBC transactions| coordination
    n3 <-->|JDBC transactions| execution
```

All three nodes run the same application and can serve the API and dashboard. The example browser URL is node 1. Each worker writes a heartbeat to `worker_node` every second. A node is active if its heartbeat is less than five seconds old. The sorted active node IDs divide **256 logical shards** by `shard % activeNodeCount`; the job UUID determines its shard. Membership changes redistribute ownership without moving job rows. PostgreSQL is required for the API, coordination, and execution.

The dashboard is served from Spring Boot's static resources. It calls the authenticated REST API and refreshes its data every 15 seconds. There is no separate frontend service or message broker. The `effect` table is the local demonstration side effect; this code does not call a cab alert service.

### End-to-end data flow

```mermaid
flowchart LR
    create[Create tenant and one-off or cron job] --> job[(job + next_fire_at + shard)]
    job --> due{DB clock says due?}
    due -->|yes and owned shard| materialize[Materialize occurrence]
    materialize --> run[(job_run: READY)]
    run --> claim[Claim with tenant slot,<br/>owner, fence, lease]
    claim --> running[(job_run: RUNNING)]
    running --> commit{Effect transaction succeeds?}
    commit -->|yes| effect[(one effect row)]
    effect --> success[(job_run: SUCCEEDED)]
    commit -->|error| retry[(job_run: RETRY)]
    retry -->|backoff elapsed| claim
    retry -->|attempt limit| dead[(job_run: DEAD)]
    dead -->|operator replay| run
```

The database clock (`clock_timestamp()`) decides when jobs are due, retries are available, and leases expire. The application clock drives polling and logs. The guarantees are **at-least-once attempts** and **one committed local effect per run**. A process may execute the attempt more than once after a crash or expired lease. An external provider needs the same idempotency key or a transactional outbox and downstream deduplication.

## Low-level design (LLD)

### Code modules and calls

```mermaid
flowchart TB
    subgraph web[HTTP layer]
        static[static/index.html + app.js + app.css]
        security[SecurityConfig<br/>HTTP Basic, BCrypt, roles]
        api[ApiController<br/>tenant, user, job, run endpoints]
        errors[ApiErrors]
    end

    subgraph scheduler[Scheduling layer, each node]
        worker[Worker<br/>heartbeat 1s<br/>poll 100ms<br/>16 execution threads]
        shards[ShardAssignment<br/>UUID to shard<br/>active-node ownership]
        fire[FireTimes<br/>tenant-zone cron calculation]
        misfire[Misfires<br/>FIRE_NOW / SKIP / COALESCE]
        backoff[Backoff<br/>jittered retry delay]
    end

    subgraph persistence[Persistence layer]
        store[SchedulerStore<br/>JDBC + TransactionTemplate]
        pg[(PostgreSQL tables and indexes)]
    end

    static --> api
    security --> api
    api --> errors
    api --> shards
    api --> fire
    api --> store
    worker --> shards
    worker --> store
    worker --> backoff
    store --> fire
    store --> misfire
    store --> pg
```

`ApiController` checks the authenticated user's tenant before tenant operations; admins can access all tenants, operators can write within their own tenant, and employees have read access. Spring Security restricts `/api/admin/**` to admins and `/actuator/**` to admins except health. The worker and API use the same `SchedulerStore` on each node.

### Database relationships

```mermaid
erDiagram
    tenant ||--o{ app_user : scopes
    tenant ||--o{ job : owns
    tenant ||--o{ tenant_slot : limits
    tenant ||--o{ job_run : owns
    tenant ||--o{ effect : owns
    job ||--o{ job_run : schedules
    job_run ||--o| effect : commits

    tenant {
        uuid id PK
        text name
        text zone
        boolean paused
        integer concurrency_cap
    }
    app_user {
        text username PK
        text password_hash
        text role
        uuid tenant_id FK
    }
    job {
        uuid id PK
        uuid tenant_id FK
        text kind
        text cron
        timestamptz next_fire_at
        text payload
        integer priority
        integer max_run_seconds
        text misfire
        integer shard
        boolean enabled
    }
    job_run {
        uuid id PK
        uuid job_id FK
        uuid tenant_id FK
        timestamptz scheduled_at
        text state
        integer attempt
        timestamptz available_at
        timestamptz lease_until
        text owner
        bigint fence
        text idempotency_key UK
        timestamptz started_at
        timestamptz completed_at
    }
    tenant_slot {
        uuid tenant_id PK,FK
        integer slot PK
        uuid run_id
        timestamptz lease_until
    }
    effect {
        text idempotency_key PK
        uuid run_id FK,UK
        uuid tenant_id
        text payload
        timestamptz committed_at
    }
    worker_node {
        text id PK
        timestamptz heartbeat_at
    }
```

`worker_node` is intentionally separate from the business-data relationships. Its rows represent heartbeats, not foreign-key ownership. An admin user may have a null `tenant_id`; operator and employee users are tenant-scoped by the API. Important constraints are `UNIQUE(job_id, scheduled_at)` on `job_run`, unique `job_run.idempotency_key`, and `effect.idempotency_key` as the primary key. `tenant_slot` has one row per allowed concurrent run. The partial `job_due_idx` supports enabled jobs by shard and due time; `job_run_claim_idx` and `job_run_tenant_queue_idx` support active run selection.

### Create, materialize, claim, commit

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant API as ApiController
    participant DB as PostgreSQL
    participant W as Worker on owning node
    participant S as SchedulerStore

    Client->>API: POST /api/tenants/{id}/jobs
    API->>DB: Check user tenant and load tenant zone
    API->>API: Validate one-off time or calculate next cron time
    API->>DB: Insert job with UUID-derived shard and next_fire_at
    API-->>Client: Job ID and next fire time

    loop Every second
        W->>S: heartbeat(node ID)
        S->>DB: Upsert worker_node heartbeat
        W->>S: activeNodes()
        S->>DB: Read heartbeats newer than 5 seconds
        W->>W: Recompute owned shards
    end

    loop Poll delay 100ms
        W->>S: materialize(owned shards)
        S->>DB: BEGIN and select up to 64 due jobs FOR UPDATE SKIP LOCKED
        S->>S: Apply tenant-zone cron and misfire rule
        S->>DB: Insert READY run, advance cursor or disable one-off job, then COMMIT
        W->>S: claimBatch(node ID, owned shards, free threads)
        S->>DB: BEGIN and select tenant-diverse candidates and free tenant slots
        S->>DB: Lock run and slot, increment attempt and fence, set owner and lease, then COMMIT
    end

    W->>S: commitEffect(run, node ID)
    S->>DB: BEGIN, lock run, and verify owner, fence, DB lease and max runtime
    S->>DB: Insert effect ON CONFLICT DO NOTHING
    S->>DB: Mark run SUCCEEDED, release tenant slot, then COMMIT
```

Materialization and claim are separate transactions. The first transaction makes a scheduled occurrence durable, while the second reserves capacity to execute it. Each poll materializes at most 64 jobs; each claim operation considers at most 64 candidate rows and fills at most the available execution threads. Candidate selection starts with one earliest eligible run per tenant, then takes at most eight runs from a selected tenant in that claim transaction. The pool has 16 threads per node; three nodes provide up to 48 simultaneous attempts, subject to tenant slot caps.

For recurring jobs, `next_fire_at` is advanced in the materialization transaction. The worker evaluates at most 500 missed occurrences per job in one pass. A run more than two seconds late follows its `FIRE_NOW`, `SKIP`, or `COALESCE` policy. Manual trigger bypasses the due-job scan and inserts a `READY` run directly.

### Run state transitions and recovery

```mermaid
stateDiagram-v2
    [*] --> READY: scheduled occurrence or manual trigger
    READY --> RUNNING: claim with slot, lease and fence
    RETRY --> RUNNING: retry available and claimed
    RUNNING --> SUCCEEDED: fenced effect transaction commits
    RUNNING --> RETRY: effect error with attempts remaining
    RUNNING --> DEAD: effect error at max attempts
    RUNNING --> RUNNING: expired lease reclaimed with higher fence
    RUNNING --> DEAD: expired lease at max attempts
    DEAD --> READY: operator replay resets attempts
    SUCCEEDED --> [*]
```

`available_at` gates retries. `Backoff` uses exponential delay with jitter and a five-minute cap. The configured maximum is five attempts and the lease lasts 15 seconds. A renewal task extends a live lease every five seconds while the worker is executing. A paused tenant is excluded from new materialization and claims; already running attempts may complete.

### Crash or freeze after a claim

```mermaid
sequenceDiagram
    autonumber
    participant A as Worker A
    participant DB as PostgreSQL
    participant B as Worker B

    A->>DB: Claim run with owner A, fence 1 and 15s lease
    Note over A: Process killed or frozen
    B->>DB: Heartbeat and recompute shard ownership
    B->>DB: After lease expiry, lock and reclaim run
    DB-->>B: Owner B with fence 2 and new lease
    B->>DB: Lock run and verify fence 2 and lease
    B->>DB: Insert effect and mark SUCCEEDED in one transaction
    Note over A: Frozen process resumes
    A->>DB: Attempt completion with fence=1
    DB-->>A: Reject stale fence and prevent second effect
```

The database row lock serializes competing completion attempts. The fence and lease check happen before the effect write, inside the same transaction as run completion. A node kill can require the five-second membership window for shard takeover and up to the 15-second run lease for a claimed run. Exactly-once *execution* is not promised; exactly one committed local effect is enforced. External effects require idempotency beyond this database.

### Capacity and verification

| Parameter | Current value | Source / purpose |
| --- | ---: | --- |
| Worker nodes in Compose | 3 | Same application image, one PostgreSQL instance |
| Logical shards | 256 | `app.shards`; partition due jobs and claims |
| Worker heartbeat / active window | 1 s / 5 s | Membership and shard assignment |
| Poll delay | 100 ms | Look for due jobs and available runs |
| Execution threads | 16 per node | Up to 48 across three nodes |
| Tenant concurrency | 1–128, benchmark uses 8 | `tenant_slot` rows |
| Claim lease / renewal | 15 s / 5 s | Takeover and live attempt protection |
| Max attempts | 5 | Retry then dead letter |
| Due jobs per materialization | 64 | Bounded database transaction |
| Measured case-study load | 100,000 jobs / 10 min / 3 nodes | 0.694 s p99 fire lag on the documented local machine |

The measured workload used 100 tenants with 1,000 jobs each, due uniformly over ten minutes. It created 100,000 `job_run` rows and 100,000 committed `effect` rows, with no dead runs. See [README.md](README.md#tests-and-benchmark) and [scripts/benchmark-100k.sh](scripts/benchmark-100k.sh) for the reproducible setup and measurement limits.
