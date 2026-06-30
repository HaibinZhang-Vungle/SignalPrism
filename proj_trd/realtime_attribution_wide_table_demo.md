# Chapter 7: Realtime Attribution Wide Table Demo

This chapter proposes a demo path for moving downstream long-range joins from the data pipeline into realtime attribution at the serving/logging layer.

The current data foundation has several related event sources:

- `delivery` / `transaction`
- no-serv transaction
- HB notification / HBN
- TPAT

The downstream problem is that these sources must be joined across a long time range by `event_id` or related request identifiers. That attribution step is expensive, fragile, and hard to make consistently available to all downstream consumers. The proposed change is to persist a compact delivery context at serving time, then enrich HBN/TPAT/no-serv records by event id as they arrive, producing a directly consumable wide event stream/table.

## 7.1 Target Outcome

The demo should prove three things before any full production rollout:

1. A compact context written by Jaeger at delivery/transaction time can be joined to later HBN/TPAT events with high hit rate.
2. A 7-day attribution window does not require keeping full delivery rows in one expensive hot KV layer.
3. A low-resource production-shadow test can work even when delivery and later notification events land in different Kubernetes clusters.

The demo output should be a shadow wide table, not a production replacement:

```
ml_shadow.realtime_attributed_event_wide
```

Suggested grain:

```
one row per emitted source event that needs attribution
```

Examples:

- one HBN notification row with delivery context duplicated onto it
- one TPAT row with delivery context duplicated onto it
- one no-serv row with request/auction context and no HBN/TPAT fields

Avoid forcing one physical row per `event_id` in the demo. TPAT/HBN can be one-to-many, and collapsing them too early hides data quality issues.

## 7.2 Important Correction: Write Context Before Outcome When Needed

If the system writes context only at `delivery`, no-serv events are not covered because they have no delivery. For the wide table to cover delivery and no-serv transaction uniformly, the stored value should be an `AttributionContext`, not just a `DeliveryContext`.

Recommended write points:

1. At auction closeout / transaction emission for served traffic.
2. At no-serv transaction emission for no-serv traffic.
3. Optionally at request parsing/enrichment for a smaller "request context" if some later events can occur before transaction emission.

The context should include an `outcome_type`:

```
served | no_serv | unknown
```

For the first demo, use the transaction/no-serv emission points. Do not write in the hot auction decision path if the write can block bidding.

## 7.3 Realtime Flow

```
Jaeger transaction/no-serv emission
  -> build compact AttributionContext
  -> async write by event_id to attribution store
  -> also emit context log to Kafka/object storage for replay and repair

HBN / TPAT event arrival
  -> compute same attribution key
  -> lookup AttributionContext by event_id
  -> if hit: emit attributed wide event
  -> if miss: emit wide event with lookup_status = miss and send to repair path

Repair job
  -> reads lookup misses
  -> joins against cold context log / source tables
  -> writes repaired wide event or miss reason
```

Jaeger writes must be best-effort and isolated:

- async write queue
- bounded memory
- timeout budget
- circuit breaker
- drop counter and sampled error logs
- never fail or slow down auction response because attribution storage is degraded

## 7.4 Attribution Key Strategy

Primary key:

```
event_id
```

Fallback keys to validate during profiling:

- `bidrequest_id`
- `imp_id`
- `req_group_id`
- `request_id`

The demo should explicitly measure whether all four source families carry the same `event_id`. If HBN/TPAT sometimes use a different id, add a key-normalization module before any storage scaling work.

Context key format:

```
attr:v1:<event_id>
```

Store versioned values:

```
{
  "schema_version": 1,
  "event_id": "...",
  "event_time_ms": 177...,
  "outcome_type": "served",
  "source_cluster": "...",
  "pub_app_object_id": "...",
  "placement_id": "...",
  "placement_type": "...",
  "supply_name": "...",
  "is_header_bidding": true,
  "rtb_connection_id": "...",
  "winner_account_id": "...",
  "dev_platform": "...",
  "geoip_country_code": "...",
  "dev_connection": "...",
  "dev_do_not_track": false,
  "dev_id_source": "...",
  "mediation_floor": 0.12,
  "predicted_nr": 0.08,
  "winning_bid_price": 0.15,
  "settlement_price": 0.13,
  "sr_pbtw": 0.14,
  "second_place_price": 0.11,
  "adomain": "...",
  "campaign_id": "...",
  "privacy_flags": {...},
  "creative_flags": {...},
  "no_serv_reason": null
}
```

Keep this as a compact protobuf/msgpack payload in production. JSON is acceptable only for the earliest local demo.

## 7.5 Wide Event Schema

Recommended common columns:

```
event_id
source_event_type             -- delivery, no_serv, hbn, tpat
source_event_time
delivery_event_time
attribution_delay_seconds
lookup_status                 -- hit, miss_sampled_out, miss_not_found, miss_expired, miss_error
context_schema_version
source_cluster
lookup_cluster
attribution_store_layer       -- l0, l1, l2, repair
```

Delivery/request context columns:

```
pub_app_object_id
placement_id
placement_type
supply_name
is_header_bidding
rtb_connection_id
winner_account_id
dev_platform
geoip_country_code
dev_connection
dev_do_not_track
dev_id_source
mediation_floor
predicted_nr
winning_bid_price
settlement_price
sr_pbtw
second_place_price
adomain
campaign_id
privacy flags
creative flags
no_serv_reason
```

HBN/TPAT-specific columns should stay source-prefixed:

```
hbn_bid_price
hbn_settlement_price
hbn_win_loss_status
hbn_loss_reason
hbn_req_group_id_at_auction
hbn_req_no_at_auction
tpat_event_name
tpat_event_start_count
tpat_event_time
```

Do not overwrite delivery fields with HBN/TPAT fields of similar names. Keep both and make derived columns explicit later.

## 7.6 Is 7-Day KV Storage Too Large?

Likely yes if interpreted as "full delivery payload for all traffic in one replicated hot KV cluster."

The docs contain multiple volume indicators:

- Jaeger serving QPS is very high.
- HBN and Modulo source tables are tens of billions of rows per day.
- Some sampled/estimated auction counts in the current docs conflict with each other, so capacity must be measured from live counters before final sizing.

Use this sizing formula:

```
7d_storage_bytes =
  daily_context_events
  * compact_payload_bytes
  * 7
  * kv_write_amplification_and_metadata_factor
  * replication_factor
```

Example only:

| Daily contexts | Compact payload | KV overhead | Replicas | 7-day physical storage |
|---:|---:|---:|---:|---:|
| 300M/day | 1KB | 2.5x | 3x | ~15.8TB |
| 5B/day | 1KB | 2.5x | 3x | ~262TB |
| 30B/day | 1KB | 2.5x | 3x | ~1.6PB |
| 300M/day | 250B | 2.5x | 3x | ~3.9TB |
| 5B/day | 250B | 2.5x | 3x | ~65.6TB |

This makes the design direction clear:

- do not store full raw delivery rows
- store only the projection required by the wide table
- use a short hot TTL for the majority of lookups
- use cheaper cold storage and repair for the long tail
- decide TTL from measured HBN/TPAT delay CDF, not from guesswork

## 7.7 Multi-Layer Storage Design

The attribution delay distribution should drive retention by layer:

```
delay = hbn_or_tpat_event_time - transaction_or_delivery_event_time
```

Measure CDF by source:

- HBN delay CDF
- TPAT delay CDF
- no-serv delay CDF if applicable
- delay by platform/country/supply/partner

Recommended layers:

| Layer | Purpose | Suggested TTL | Storage | Read path |
|---|---|---:|---|---|
| L0 in-process/sidecar cache | duplicate/retry absorption | 5-30 min | memory/Ristretto/RocksDB | sync |
| L1 hot KV | normal online attribution | 6-24h | KVRocks/Redis/Dragonfly/Scylla | sync |
| L2 warm KV/log state | late but still common events | 2-7d | KVRocks on SSD, RocksDB state, Cassandra/Scylla, compacted Kafka + state store | sync or bounded async |
| L3 cold context log | repair and audit | 7-30d | Kafka archive + Iceberg/S3 | async repair |

The demo should start with L1 + L3 only:

- L1: small KVRocks/Redis/RocksDB deployment for sampled keys.
- L3: append every sampled context to a Kafka topic or Iceberg table for deterministic replay.

Add L2 only if the measured delay CDF shows meaningful business value beyond the L1 TTL.

## 7.8 Cross-Cluster Problem and POC Strategy

Do not test by Kubernetes cluster.

A cluster-scoped test is invalid because delivery can be produced in one cluster while HBN/TPAT arrives in another. The sample unit must be a globally stable event cohort.

Use deterministic event-id sampling:

```
sampled = hash64(event_id, seed) % 1_000_000 < sample_threshold
```

Start thresholds:

- 0.001% for deploy safety and payload validation
- 0.01% for first meaningful hit-rate metrics
- 0.1% only after storage and lookup latency are stable

The same sampling function must run in all writer and reader code paths:

- Jaeger transaction writer: write context only if sampled.
- HBN/TPAT reader: lookup/emit shadow wide row only if sampled.
- Repair job: process sampled misses only.

This solves the cross-cluster issue because every cluster makes the same decision for the same `event_id`. A HBN event in cluster B will still look up the context written by delivery in cluster A because both are in the same global sampled cohort.

For the POC, use one logical attribution store shared by all clusters for the sampled keyspace. It can be low spec because the sample rate is tiny.

## 7.9 Recommended POC Phases

### Phase 0: Offline Replay, No Production Change

Goal: validate keys, schema, delay distribution, and required TTL.

Input:

- historical delivery/transaction rows
- no-serv transaction rows
- HBN rows
- TPAT rows

Method:

1. Select a global event-id hash sample, for example 0.01%.
2. Sort or bucket rows by event time.
3. Write sampled transaction/no-serv contexts to local RocksDB/KVRocks.
4. Replay HBN/TPAT events and perform lookup.
5. Emit a local wide table.
6. Compare against existing offline join output as the golden dataset.

Outputs:

- key coverage by source
- hit rate by TTL bucket
- payload bytes p50/p95/p99
- estimated storage for 6h/24h/48h/7d
- one-to-many cardinality report
- wide schema draft

### Phase 1: Production Shadow Write

Goal: prove Jaeger can write compact context safely.

Behavior:

- deploy writer to all clusters
- event-id sample at 0.001%-0.01%
- async write only
- no lookup in request path
- no production consumer changes
- append context log for replay

Success criteria:

- write error rate below threshold
- queue drops understood and bounded
- p99 write latency not visible in auction latency
- storage/day matches estimate
- context schema has all fields needed for demo wide table

### Phase 2: Production Shadow Lookup

Goal: prove cross-cluster online attribution.

Behavior:

- deploy HBN/TPAT lookup to all relevant clusters
- same event-id sample gate
- emit `ml_shadow.realtime_attributed_event_wide`
- do not replace existing pipelines

Success criteria:

- hit rate meets TTL expectation
- misses are explainable: sampled out, no key, expired, source key mismatch, write failure
- lookup p99 within budget
- shadow wide rows match offline join for sampled cohort

### Phase 3: Layered TTL Experiment

Goal: find cost/performance tradeoff.

Run A/B by sample sub-cohort:

| Cohort | L1 TTL | L2/Warm | Repair | Purpose |
|---|---:|---|---|---|
| A | 6h | none | yes | cheap baseline |
| B | 24h | none | yes | likely practical hot window |
| C | 24h | 7d compact | yes | long-tail online hit-rate |

Compare:

- incremental hit rate from longer TTL
- additional storage cost
- repair volume reduction
- downstream freshness improvement

### Phase 4: Consumer Demo

Goal: show why the wide table is useful.

Build one downstream query or dashboard that reads only the shadow wide table and reproduces a metric currently requiring long-range joins:

- HBN win/loss + delivery context
- TPAT event_start + app/device/placement context
- served vs no-serv funnel by placement/platform/country
- B1/B2/B3 style shading fields with delivery dimensions attached

## 7.10 Low-Resource Environment Recommendation

For the demo, do not start with a production-scale KVDB. Use this stack:

```
sampled Jaeger context writer
  -> small shared attribution-store service
  -> KVRocks/Redis/RocksDB backend
  -> sampled HBN/TPAT lookup path
  -> shadow Kafka topic
  -> Iceberg shadow wide table
```

Resource control levers:

- event-id hash sample
- source filters, for example only HBN + TPAT at first
- payload projection
- TTL cap
- per-cluster QPS limiter
- per-source budget
- store only sampled contexts

Because the sample is global by event id, not by cluster, this validates the cross-cluster attribution behavior with a tiny fraction of total traffic.

## 7.11 Metrics Required for Go/No-Go

Core attribution metrics:

```
context_write_qps
context_write_error_rate
context_write_drop_rate
lookup_qps
lookup_hit_rate
lookup_miss_rate_by_reason
lookup_latency_p50/p95/p99
attribution_delay_seconds_p50/p90/p95/p99/p999
payload_bytes_p50/p95/p99
wide_row_count_by_source_event_type
duplicate_context_write_rate
multiple_hbn_or_tpat_per_event_id_rate
offline_join_match_rate
```

Capacity metrics:

```
stored_keys
logical_bytes
physical_bytes
bytes_per_context
compaction_cpu
eviction_count
expired_before_lookup_count
repair_backlog
repair_success_rate
```

Quality gates:

- `event_id` exists and is stable across sources for the sampled cohort.
- Online attributed wide rows match offline join output within an agreed tolerance.
- Missing attribution is mostly explainable by TTL/sample/source-key issues.
- Auction latency is unaffected.
- Storage projection for full traffic is understood before raising sample rate.

## 7.12 Main Risks

| Risk | Why it matters | Mitigation |
|---|---|---|
| `event_id` is not universal across sources | lookup fails even with enough storage | Phase 0 key coverage report and fallback key mapping |
| delivery-only write misses no-serv | no-serv wide table remains incomplete | write `AttributionContext` at transaction/no-serv emission |
| full 7-day hot KV is too expensive | demo cannot scale | compact projection + layered TTL + repair |
| cross-cluster traffic breaks cluster-scoped tests | false negative POC | global event-id hash sample across all clusters |
| HBN/TPAT arrives before context write | asymmetric lookup misses | retry queue or two-sided stream join/repair |
| payload schema drifts | wide table columns become unreliable | versioned context schema and compatibility checks |
| storage outage affects serving | production risk | async best-effort writes, circuit breaker, no serving dependency |

## 7.13 Recommendation

The best demo is not "turn on 7-day KV for production traffic." It is:

1. Run an offline replay to measure key coverage, delay CDF, payload size, and correctness.
2. Deploy a global event-id sampled production shadow writer to all clusters.
3. Add sampled HBN/TPAT lookup and emit a shadow wide table.
4. Test 6h vs 24h vs 7d layered retention on sub-cohorts.
5. Keep repair through cold context logs so online KV only handles the economically useful part of the latency distribution.

This validates the architecture with low resources while directly addressing the real production constraint: delivery and later attribution events do not necessarily land in the same Kubernetes cluster.
