# Stateful regex-consumer lifecycle — before & after

**Scope:** `uncommitted working tree` (tracked and untracked changes present before this document was generated)
**Core files:** `lib/pulsar_ex/consumer_manager.ex`, `lib/pulsar_ex/consumer.ex`, `lib/pulsar_ex/application.ex`, `lib/pulsar_ex.ex`, `lib/pulsar_ex/admin.ex`, `lib/pulsar_ex/partition_manager.ex`

The change turns regex-based consumer discovery from a stateless periodic restart attempt into a reconciled lifecycle. The manager now remembers matched topics, rejects duplicate starts, ignores stale refresh messages, stops topics that disappear, and exposes lifecycle telemetry and consumer gauges.

| Area | What changed | Review effort |
|---|---|---|
| `lib/pulsar_ex/consumer_manager.ex` | Lifecycle state, topic reconciliation, race-safe timers, stop fallback, telemetry, desired counts | highest |
| `lib/pulsar_ex/consumer.ex` and `lib/pulsar_ex/application.ex` | READY registration | medium |
| `lib/pulsar_ex.ex` | Desired/active/ready gauge API | low |
| `lib/pulsar_ex/admin.ex` and `lib/pulsar_ex/partition_manager.ex` | Admin helpers/body handling and configurable partition watch interval | supporting |
| `config/`, `test/`, `mix.exs`, `.gitignore` | Integration/load configuration, test doubles, and expanded lifecycle/load coverage | verification |

## Before

Discovery could start newly appearing topics, but it did not retain enough state to reconcile removals or protect the lifecycle from duplicate and stale timers.

```mermaid
flowchart TD
  S["Start regex consumer"] --> D["Discover topics matching tenant, namespace, and topic selectors"]
  D -->|error| E["Return error; no refresh lifecycle starts"]
  D -->|success| C["Look up partitions and start missing consumers"]
  C --> T["Schedule refresh and store only its timer reference"]
  T --> R["Refresh fires without checking whether lifecycle is still current"]
  R --> D

  S -->|same key again| L["Run discovery again and replace the stored timer reference"]
  L --> D
  L --> O["Older timer remains live"]

  P["Stop regex consumer"] --> X["Cancel only the currently stored timer"]
  X --> Q["Run discovery again"]
  Q -->|success| K["Stop only topics returned now"]
  Q -->|error or deleted topic omitted| U["Previously started consumers can remain"]
  X --> V["Already-queued refresh can run and recreate the lifecycle"]
```

## After

The manager owns an explicit lifecycle keyed by the consumer pattern, including a matched-topic snapshot and a refresh-generation token.

```mermaid
flowchart TD
  S["Start regex consumer"] --> I{"Lifecycle key already present?"}
  I -->|yes| N["Return ok without discovery or another timer"]
  I -->|no| D["Discover matching topics"]
  D -->|error| E["Emit discovery error telemetry and return error"]
  D -->|success| C["Start consumers for every discovered topic and partition"]
  C --> P["Store matched topics and desired-consumer snapshot"]
  P --> T["Schedule refresh with a generation token"]

  T --> R["Refresh message arrives"]
  R --> G{"Key and generation token still current?"}
  G -->|no| X["Ignore stale refresh"]
  G -->|yes| D2["Rediscover topics"]
  D2 -->|error| K["Keep prior snapshot and emit error telemetry"]
  D2 -->|success| A["Start new or missing consumers"]
  A --> Q["Diff old and new matched-topic sets"]
  Q --> Z["Stop removed topics; update snapshot; emit completion telemetry"]
  K --> T
  Z --> T

  P2["Stop regex consumer"] --> M["Remove lifecycle state and cancel timer"]
  M --> F{"Stored matched topics available?"}
  F -->|yes| H["Stop consumers from the snapshot"]
  F -->|no, such as manager restart| J["Scan the registry and stop selector matches"]
```

## What changed

The old timer-only paths are red and dotted. The new state and validation steps are green; yellow nodes retain their role but now operate with lifecycle state and telemetry.

```mermaid
flowchart TD
  S["Start request"]:::same --> I{"Lifecycle key exists?"}:::added
  S -.-> O["Always rediscover and overwrite the tracked timer"]:::removed
  I -->|new key| D["Discover matching topics"]:::changed
  I -->|existing key| N["Return ok as a no-op"]:::added
  O -.-> D
  D -->|error| E["Emit discovery error telemetry"]:::added
  D -->|success| C["Start topic and partition consumers"]:::same
  C --> P["Save matched topics and desired count"]:::added
  P --> T["Schedule tokenized refresh"]:::changed
  T --> R["Refresh arrives"]:::changed
  R --> G{"Key and token current?"}:::added
  G -->|stale| X["Ignore message"]:::added
  G -->|current| D2["Rediscover"]:::changed
  D2 -->|error| K["Retain old snapshot and report failure"]:::added
  D2 -->|success| A["Start new or missing consumers"]:::same
  A --> Q["Diff previous and current topic sets"]:::added
  Q --> Z["Stop removed topics; update snapshot and telemetry"]:::added
  Z --> T

  Stop["Stop request"]:::changed --> M["Remove lifecycle and cancel timer"]:::added
  M --> SS["Stop snapshotted topics or registry matches"]:::added
  Stop -.-> RD["Rediscover during stop"]:::removed
  RD -.-> SO["Stop only topics returned at stop time"]:::removed

  classDef added fill:#d4f8d4,stroke:#2ea043,color:#1f2328
  classDef removed fill:#ffd7d5,stroke:#cf222e,color:#1f2328
  classDef changed fill:#fff5cc,stroke:#bf8700,color:#1f2328
  classDef same fill:#f6f8fa,stroke:#8c959f,color:#1f2328
  linkStyle 1,4,20,21 stroke:#cf222e,stroke-dasharray:5
```

**Legend** — 🟩 added · 🟥 removed · 🟨 modified · ⬜ unchanged.  
Dotted red edges are lifecycle paths that no longer exist.

## Zoom: consumer gauges

A consumer registers as ready only after a successful subscription. The gauge API
combines that registry state with active consumer IDs and manager snapshots.

```mermaid
flowchart LR
  C["Consumer subscription succeeds"]:::same --> R["Register READY"]:::added

  G["consumer_gauges query"]:::added --> GD["Desired from manager snapshots"]:::added
  G --> GA["Active from consumer ID registry"]:::added
  G --> GR["Ready from READY registry"]:::added

  classDef added fill:#d4f8d4,stroke:#2ea043,color:#1f2328
  classDef same fill:#f6f8fa,stroke:#8c959f,color:#1f2328
```

## Notes

- Exact-topic consumers share the new tokenized refresh protection and desired-count snapshots, but topic-set reconciliation applies only to regex discovery.
- On refresh discovery failure, the previous matched-topic set and desired snapshot are retained; the manager reports the error and schedules the next refresh.
- The diagram omits the new integration/load harness and most test-support modules. Those files verify discovery, partition fanout/expansion, deletion, duplicate starts, stop races, telemetry, gauges, and load behavior rather than adding production control-flow steps.
- `lib/pulsar_ex/admin.ex` also adds partitioned-topic update/delete helpers and fixes response-body consumption in existing admin calls; these are independent supporting changes and are not shown in the lifecycle graph.
