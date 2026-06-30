# WideTable Attribution Development Spec

## 0. Objective

Implement the realtime HB delivery / HB notification wide attribution POC in the Jaeger repository:

- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger`

This spec is written for AI coding agents. All referenced files use absolute paths so the spec can be run from different working directories without editing the wrong repository.

Design references:

- `/Users/haibinzhang/workspace/go/src/github.com/hbzhang/SignalPrism/proj_trd/realtime_attribution_wide_table_demo.md`
- `/Users/haibinzhang/workspace/go/src/github.com/hbzhang/SignalPrism/proj_trd/end_to_end_mlops_wide_table_demo.md`

Target behavior:

1. After a successful Jaeger HB delivery, write the full HB transaction message asynchronously to a Redis/KVRocks-compatible KVDB.
2. When Scrat receives an HB notification, look up the transaction by `event_id + imp_id`, assemble a wide notification message, and write it to the new Kafka topic `hb-notification-wide-202600630`.
3. Keep the existing HB notification topic, payload, and write behavior unchanged.
4. Use a KVDB dedupe key so the same `event_id + imp_id + notification_type` produces at most one wide row.
5. Set both transaction keys and dedupe keys to a 3-hour TTL.

## 1. Confirmed Code Locations

Before coding, read these files and confirm the referenced logic is still current.

### Jaeger HB Delivery / Transaction Path

- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/router/handler/hb/record.go`
  - `recordMessages(data *hbdata.HBData)` is the HB handler message-recording entry point.
  - Current order: on successful realtime delivery it calls `r.WriteAdDeliveryMessage()`, on valid transaction it calls `r.WriteAdTransactionMessage()`, and it always calls `r.WriteHBTransactionMessage()` at the end.
- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/router/handler/csb/handler.go`
  - `record(data *hbdata.HBData)` also uses `hbrecorder.New(data)` and writes delivery, transaction, and HB transaction messages.
  - If CSB traffic is part of this HB delivery scope, apply the same wide attribution writer gate there.
- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/hbp/hbrecorder/recorder.go`
  - This composes `hbtransactionrecorder.Recorder`, `adtransaction.Recorder`, and `addelivery.Recorder`.
  - Prefer adding a small API here to register an additional HB transaction receiver instead of reaching into `hbtransactionrecorder` internals from handlers.
- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/hbp/hbtransactionrecorder/recorder.go`
  - `buildMessage()` builds the full `*hbtransaction.Message`.
  - `New()` configures the default receiver that writes the existing `TopicHBTransaction` or `TopicHBTransactionNoServ`.
  - `msg.EventID = r.src.BidID()` and `msg.BidID = r.src.BidID()`.
- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/hbp/hbtransaction/hbtransaction.go`
  - Defines the HB transaction message struct and already uses easyjson.
- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/apiconv/defaults/defaults.go`
  - Defines `DefaultImpressionID = "1"`.

### Scrat HB Notification Path

- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/cmd/scrat/scrat.go`
  - The `hbp_notifications` endpoint mounts `/api/v5/impression`, `/api/v5/win`, `/api/v5/load_ad`, `/api/v5/loss`, `/api/v5/bill`, `/api/v5/timeout`, and legacy `/api/*` paths.
- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/scrat/scrat/handler/hbpnotification/hbpnotification.go`
  - `Handler.onNotification` parses `HBNotificationMessage`.
  - The existing async routine calls `messagehelper.WriteMessage(msg)` to write the original HB notification.
  - The new wide logic must run as a side path and must not mutate the original `msg` payload sent to the existing topic.
- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/scrat/hbpreport/message.go`
  - `HBNotificationMessage` already contains `EventID`, `ImpressionID`, `NotificationType`, price fields, loss/win/bill fields, and related notification data.
- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/scrat/messagehelper/config.go`
  - Scrat Kafka topic envconfig lives here.
- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/scrat/messagehelper/topic.go`
  - Scrat topic enum lives here.
- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/scrat/messagehelper/messagewriter.go`
  - Topic name mapping and topic registration live here.

### Helm / Runtime Config

- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/_ops/argocd/charts/jaeger/jaeger/values.yaml`
- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/_ops/argocd/charts/scrat/scrat/values.yaml`
- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/_ops/argocd/charts/jaeger/jaeger/templates/rollout.yaml`
- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/_ops/argocd/charts/scrat/scrat/templates/rollout.yaml`

## 2. Scope

In scope:

- Header bidding only.
- Successful delivery cases only.
- Jaeger writes the full `*hbtransaction.Message` to KVDB.
- Scrat handles HB notification wide attribution only.
- KVDB endpoint: `prod-agg-device-feature.rocksdb.vungle.io:6379`.
- KVDB has no password for this POC.
- New Kafka topic: `hb-notification-wide-202600630`.
- The new topic must use the non-revenue-related Kafka cluster, which is the local Kafka cluster.

Out of scope:

- Do not replace the existing HB transaction topic.
- Do not replace the existing HB notification topic.
- Do not change the existing HB notification payload.
- Do not implement a 7-day TTL.
- Do not implement L2/L3 repair.
- Do not handle TPAT, no-serv, S2S expansion, or non-HB traffic.

## 3. KVDB Key Contract

### Transaction Key

Store the full HB transaction message under:

```text
wt:hbtxn:v1:<escaped_event_id>:<escaped_imp_id>
```

TTL: `3h`, exactly `10800` seconds.

Value:

- Serialize the full `*hbtransaction.Message` with `easyjson.Marshal`.
- Do not use `encoding/json` in this hot path.
- If a metadata wrapper is needed, add `//go:generate easyjson $GOFILE` and `//easyjson:json`, then commit the generated `_easyjson.go`.

`imp_id` selection rule:

1. Prefer `data.RawBidRequest().Impressions[0].ID`.
2. If it is empty and the current HB notification flow uses the default impression ID, use `defaults.DefaultImpressionID` from `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/apiconv/defaults/defaults.go`.
3. If `event_id` or final `imp_id` is empty, skip the KVDB write and record a metric. Do not write malformed keys.

Key parts must be escaped. Do not concatenate raw `eventID + ":" + impID` without escaping. Use `url.PathEscape`, `base64.RawURLEncoding`, or another deterministic helper. The same helper must be shared by the Jaeger writer and Scrat reader.

### Dedupe Key

Before Scrat writes the wide topic, use a KVDB key for atomic dedupe:

```text
wt:hbnwide:dedupe:v1:<escaped_event_id>:<escaped_imp_id>:<escaped_notification_type>
```

TTL: `3h`, exactly `10800` seconds.

Use Redis/KVRocks atomic semantics:

```text
SET key value NX EX 10800
```

Behavior:

- If `SET NX EX` succeeds, write the wide topic.
- If the key already exists, record a duplicate metric and do not write the wide topic.
- If KVDB returns an error, do not write the wide topic. Record an error metric. Do not affect the original HB notification write.
- If the wide Kafka write fails after dedupe succeeds, best-effort delete the dedupe key so a later retry can write the wide row. If delete fails, only record a log/metric.
- Do not set the dedupe key on lookup miss because no wide row was written.

## 4. Shared Package

Add a shared package:

- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/wideattribution`

This package must be used by both Jaeger and Scrat so key format, escaping, TTL, config, and store semantics cannot drift.

Suggested files:

- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/wideattribution/key.go`
  - `TransactionKey(eventID, impID string) (string, bool)`
  - `DedupeKey(eventID, impID, notificationType string) (string, bool)`
  - `ImpressionIDFromOpenRTB(req *openrtb.BidRequest) string`
- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/wideattribution/config.go`
  - Envconfig config for endpoint, timeouts, pool size, enabled flag, and TTL seconds.
  - Jaeger namespace: `JAEGER_WIDE_ATTRIBUTION_*`.
  - Scrat namespace: `SCRAT_WIDE_ATTRIBUTION_*`.
- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/wideattribution/store.go`
  - Redis/KVRocks client wrapper using `github.com/redis/go-redis/v9` or an existing local Redis utility if it cleanly supports this standalone endpoint.
  - Required operations: `SetTransaction(ctx, key, []byte, ttl)`, `GetTransaction(ctx, key)`, `SetDedupeNX(ctx, key, ttl)`, `DeleteDedupe(ctx, key)`, and `Close()`.
  - Client initialization must not panic on ping failure in the production request path. Record unavailable state and metrics instead.
- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/wideattribution/metric.go`
  - Shared metric names and labels, implemented using the existing repository Prometheus style.

If an existing package is clearly a better fit after inspection, reuse it, but keep the key helpers shared and keep attribution KVDB prefixes independent from existing bid cache or notice cache prefixes.

## 5. Jaeger Writer Implementation

### Service Init

Add service initialization and shutdown in:

- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/cmd/jaeger/jaeger.go`

Add the wide attribution writer as a non-revenue-impact service after existing Redis/KVRocks dependencies are initialized. Stop it during service shutdown.

The writer must be best effort:

- Use a bounded channel queue.
- Use a small fixed worker count.
- Enqueue from request/message paths without blocking.
- Apply a timeout to each KVDB write.
- Record a drop metric when the queue is full.
- Use sampled error logs.
- Do not panic when KVDB is unavailable.

### Hook Point

Preferred implementation:

1. Add this method in `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/hbp/hbrecorder/recorder.go`:

```go
RegisterHBTransactionReceiver(f hbtransactionrecorder.ReceiveHBTransactionFunc)
```

2. In `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/router/handler/hb/record.go`, register the receiver before `r.WriteHBTransactionMessage()` only when all conditions are true:

- `data.IsRealtime() == true`
- `data.EnrichedRequest() != nil`
- `data.NoServReason.IsNone() == true`
- `data.EnrichedRequest().IsTestMode() == false`
- `data.RawBidRequest() != nil`
- `event_id` and `imp_id` can be computed

3. In `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/router/handler/csb/handler.go`, apply the same receiver gate if CSB is part of header-bidding successful delivery traffic.

4. The receiver must enqueue the complete `*hbtransaction.Message` plus the computed `imp_id` into the async writer. It must not write KVDB inline.

Do not add unconditional wide writes inside `hbtransactionrecorder.New()` unless the code also has a strict successful-delivery gate. `WriteHBTransactionMessage()` currently runs for no-serv and other cases, so an unconditional receiver would violate this scope.

### Jaeger Metrics

Add at least:

- `wide_attribution_transaction_kvdb_write_total{status="success|error|skipped|dropped"}`
- `wide_attribution_transaction_kvdb_write_bytes_total`
- `wide_attribution_transaction_kvdb_write_size_bytes` as a histogram or summary
- `wide_attribution_transaction_writer_queue_depth` as a gauge if practical

Record data size as the serialized byte length of the transaction value actually sent to KVDB.

## 6. Scrat Lookup and Wide Topic Implementation

### Topic Config

Add a new Scrat topic enum:

- File: `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/scrat/messagehelper/topic.go`
- Name: `TopicHBNotificationWide`
- String value: `"TopicHBNotificationWide"`

Add envconfig:

- File: `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/scrat/messagehelper/config.go`
- Field: `TopicHBNotificationWide string`
- Envconfig name: `hbNotificationWide`
- Default value: `hb-notification-wide-202600630`

Register the topic:

- File: `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/scrat/messagehelper/messagewriter.go`
- Add `TopicHBNotificationWide: DefaultConfig.Kafka.Topics.TopicHBNotificationWide`.

Helm values:

- File: `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/_ops/argocd/charts/scrat/scrat/values.yaml`
- Add `SCRAT_KAFKA_TOPIC_HBNOTIFICATION_WIDE: "hb-notification-wide-202600630"` near the existing HBN topic.

Kafka routing:

- File: `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/_ops/argocd/charts/scrat/scrat/templates/rollout.yaml`
- Ensure `hb-notification-wide-202600630` is included in `SCRAT_KAFKA_NON_CRITICAL_TOPIC_LIST`.
- Do not route the new topic through revenue or critical Kafka.

### Wide Message Schema

Add a new Scrat package:

- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/scrat/hbnotificationwide`

Suggested files:

- `message.go`
- `assembler.go`
- `metric.go`
- tests

`HBNotificationWideMessage` must use easyjson and either implement `messagetype.Message` or be written through `messagehelper.NewMessage`.

Minimum JSON fields:

- `schema_version` = `1`
- `event_id`
- `imp_id`
- `notification_type`
- `source_event_type` = `"hbn"`
- `source_event_time`
- `lookup_status` = `"hit"`
- `attribution_delay_seconds`, if the transaction timestamp can be parsed
- `dedupe_key`

Include HBN source fields with an `hbn_` prefix for the core notification data:

- `hbn_bid_id`
- `hbn_supply_name`
- `hbn_bid_price`
- `hbn_bflat_bid_price`
- `hbn_settlement_price`
- `hbn_second_highest_bid_price`
- `hbn_loss_reason`
- `hbn_bid_won`
- `hbn_is_bill`
- `hbn_bid_mode`
- `hbn_is_s2s`
- `hbn_mediation_floor`

Include transaction context:

- Include the full unmarshaled `*hbtransaction.Message` under `transaction`.
- Also expose important join/debug fields at top level when available:
  - `transaction_event_id`
  - `transaction_bid_id`
  - `transaction_pub_app_object_id`
  - `transaction_pub_app_id`
  - `transaction_placement_id`
  - `transaction_placement_reference_id`
  - `transaction_supply_name`
  - `transaction_is_realtime`
  - `transaction_device_id_source`
  - `transaction_country`
  - `transaction_rtb_id`, or bidder id if that is the available field

Do not overwrite transaction fields with HBN fields that have similar names. Keep source prefixes.

### Handler Hook

In `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/scrat/scrat/handler/hbpnotification/hbpnotification.go`:

- Keep the existing `messagehelper.WriteMessage(msg)` routine unchanged for the original HBN write.
- Add a separate best-effort call after `msg` has been parsed and amended.
- The wide path may run in the same async routine after the original write or in a separate bounded worker, but failure must never affect the HTTP response or original HBN write.
- Use `msg.EventID`, `msg.ImpressionID`, and `msg.NotificationType.String()` for lookup and dedupe.
- Skip wide output for empty `event_id` or `imp_id` and record a metric.
- Current scope is HB notification. Do not include timeout handling unless it produces a normal `HBNotificationMessage` with event, impression, and type.

Processing order:

1. Build the transaction key from `msg.EventID + msg.ImpressionID`.
2. Get transaction bytes from KVDB.
3. On miss, increment the miss metric and return. Do not set dedupe.
4. On hit, unmarshal into `hbtransaction.Message`.
5. Build the dedupe key from `event_id + imp_id + notification_type`.
6. Execute `SET NX EX` for the dedupe key.
7. If duplicate, increment the duplicate metric and return.
8. Build `HBNotificationWideMessage`.
9. Write to `TopicHBNotificationWide`.
10. If Kafka write fails, best-effort delete the dedupe key and increment the write error metric.

### Scrat Metrics

Add at least:

- `hb_notification_wide_lookup_total{status="hit|miss|error|skipped"}`
- `hb_notification_wide_dedupe_total{status="new|duplicate|error"}`
- `hb_notification_wide_write_total{status="success|error"}`

The user explicitly requires metrics for found and not-found event counts:

- Found maps to `hb_notification_wide_lookup_total{status="hit"}`.
- Not found maps to `hb_notification_wide_lookup_total{status="miss"}`.

## 7. Helm KVDB Config

Hardcode the POC endpoint in Helm values as requested.

Jaeger values:

- File: `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/_ops/argocd/charts/jaeger/jaeger/values.yaml`
- Add near existing KVRocks config:

```yaml
JAEGER_WIDE_ATTRIBUTION_ENABLED: "true"
JAEGER_WIDE_ATTRIBUTION_KVDB_NODE_ADDRESSES: "prod-agg-device-feature.rocksdb.vungle.io:6379"
JAEGER_WIDE_ATTRIBUTION_KVDB_MAX_RETRIES: "0"
JAEGER_WIDE_ATTRIBUTION_KVDB_READ_TIMEOUT_MILLISECONDS: "50"
JAEGER_WIDE_ATTRIBUTION_KVDB_WRITE_TIMEOUT_MILLISECONDS: "50"
JAEGER_WIDE_ATTRIBUTION_KVDB_DIAL_TIMEOUT_MILLISECONDS: "1000"
JAEGER_WIDE_ATTRIBUTION_KVDB_POOL_SIZE_PER_NODE: "5"
JAEGER_WIDE_ATTRIBUTION_TTL_SECONDS: "10800"
JAEGER_WIDE_ATTRIBUTION_WRITER_QUEUE_SIZE: "10000"
JAEGER_WIDE_ATTRIBUTION_WRITER_WORKERS: "2"
```

Scrat values:

- File: `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/_ops/argocd/charts/scrat/scrat/values.yaml`
- Add:

```yaml
SCRAT_WIDE_ATTRIBUTION_ENABLED: "true"
SCRAT_WIDE_ATTRIBUTION_KVDB_NODE_ADDRESSES: "prod-agg-device-feature.rocksdb.vungle.io:6379"
SCRAT_WIDE_ATTRIBUTION_KVDB_MAX_RETRIES: "0"
SCRAT_WIDE_ATTRIBUTION_KVDB_READ_TIMEOUT_MILLISECONDS: "50"
SCRAT_WIDE_ATTRIBUTION_KVDB_WRITE_TIMEOUT_MILLISECONDS: "50"
SCRAT_WIDE_ATTRIBUTION_KVDB_DIAL_TIMEOUT_MILLISECONDS: "1000"
SCRAT_WIDE_ATTRIBUTION_KVDB_POOL_SIZE_PER_NODE: "5"
SCRAT_WIDE_ATTRIBUTION_TTL_SECONDS: "10800"
SCRAT_KAFKA_TOPIC_HBNOTIFICATION_WIDE: "hb-notification-wide-202600630"
```

No password or username env is required for this endpoint. Do not add secret references unless a live connection test proves authentication is required.

## 8. Preconditions Before Coding

Before edits:

1. Run `git status -sb` in `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger` and preserve unrelated user changes.
2. Confirm the files listed in section 1 still contain the referenced functions.
3. Confirm there is no existing wide attribution implementation:

```sh
rg -n "wide_attribution|hb_notification_wide|hb-notification-wide|WideAttribution" /Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger
```

4. Run targeted baseline tests if feasible:

```sh
go test ./internal/hbp/hbrecorder ./internal/hbp/hbtransactionrecorder ./internal/router/handler/hb ./internal/router/handler/csb ./internal/scrat/scrat/handler/hbpnotification ./internal/scrat/messagehelper ./internal/scrat/hbpreport
```

5. If baseline tests already fail, record the failures before changing code.

## 9. Guardrails

- No request path may block on KVDB writes.
- No auction latency regression: Jaeger enqueue must be bounded and non-blocking.
- No original HBN behavior change: the existing `messagehelper.WriteMessage(msg)` call must still receive the same `HBNotificationMessage`.
- No wide topic write may go to revenue Kafka. The new topic must be in the non-critical/local topic list.
- No silent duplicate wide rows: use atomic `SET NX EX` dedupe.
- No malformed keys: skip empty event id or impression id.
- No permanent storage: transaction and dedupe keys must use exactly `10800` seconds TTL.
- No secrets in code or Helm values. The endpoint has no password.
- No unbounded goroutine per event in the Jaeger writer.
- No `encoding/json` for new serialized structs in hot message paths. Use easyjson or existing easyjson structs.
- Do not log full transaction payloads, device IDs, tokens, or raw ext values on errors.
- Log event ids only if existing logging policy allows it. Otherwise sample and sanitize.
- KVDB outage must degrade to metrics/logs only. It must not fail delivery, notification response, or original Kafka write.

## 10. Unit Tests

### Shared Key / Store Package

Add tests under:

- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/wideattribution`

Required cases:

- Transaction key uses event id and impression id.
- Transaction key escapes separators and unsafe key characters.
- Dedupe key uses event id, impression id, and notification type.
- Empty event id or impression id returns `ok=false`.
- Default TTL is `10800` seconds.
- `SET NX EX` duplicate behavior is covered through a fake or mock Redis client.
- Transaction get miss maps to miss status, not a hard error.

### Jaeger Writer

Required cases:

- Successful realtime HB delivery enqueues one full HB transaction.
- No-serv does not enqueue.
- Test mode does not enqueue.
- Missing raw bid request or missing impression id skips and records a metric.
- Queue full drops and does not block.
- Serialized size metric uses the actual marshaled byte length.

Suggested touched tests:

- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/hbp/hbrecorder/recorder_test.go`
- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/router/handler/hb/hbtransaction_test.go`
- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/router/handler/hb/handler_test.go`
- Related CSB tests if `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/router/handler/csb/handler.go` is changed.

### Scrat Wide Message / Handler

Required cases:

- Hit plus new dedupe writes exactly one `TopicHBNotificationWide` message.
- Duplicate dedupe does not write a wide message.
- Lookup miss records miss and does not write a wide message.
- KVDB error records error and does not write a wide message.
- Wide Kafka write failure deletes the dedupe key best effort.
- Original HBN write is still called with an unchanged message.
- New topic enum maps to `hb-notification-wide-202600630`.

Suggested touched tests:

- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/scrat/scrat/handler/hbpnotification/hbpnotification_test.go`
- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/scrat/messagehelper/topic_test.go`
- `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/scrat/messagehelper/messagewriter_test.go`
- New tests under `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger/internal/scrat/hbnotificationwide`

## 11. Regression

After implementation, run at minimum from `/Users/haibinzhang/workspace/go/src/github.com/Vungle/jaeger3/jaeger`:

```sh
go test ./internal/wideattribution/... ./internal/hbp/hbrecorder ./internal/hbp/hbtransactionrecorder ./internal/router/handler/hb ./internal/router/handler/csb ./internal/scrat/hbnotificationwide ./internal/scrat/scrat/handler/hbpnotification ./internal/scrat/messagehelper ./internal/scrat/hbpreport
```

If easyjson annotations were added or changed, run:

```sh
make generate-stub target=<absolute-or-relative-go-file-that-has-easyjson-annotation>
```

Then rerun the relevant package tests.

Also run:

```sh
git diff --stat
git diff --check
```

## 12. Post-Implementation Verification

Verify all of the following:

- Jaeger writes transaction KV only for the successful HB delivery gate.
- KV transaction value is the full `hbtransaction.Message`, not a reduced projection.
- Transaction key TTL is 3 hours.
- Scrat uses the same transaction key helper.
- Scrat hit/miss metrics are present.
- Scrat dedupe key uses `event_id + imp_id + notification_type`.
- Dedupe key TTL is 3 hours.
- The original HBN topic name is unchanged.
- The original `HBNotificationMessage` content is unchanged.
- The new topic is `hb-notification-wide-202600630`.
- The new topic is routed through the non-critical/local Kafka cluster.
- No wide write path can panic on nil message, nil transaction, empty IDs, Redis nil, or Kafka write failure.
- No generated easyjson file is missing.

## 13. Edge Cases

Handle explicitly:

- Empty `event_id`.
- Empty `imp_id`.
- `RawBidRequest()` is nil.
- `RawBidRequest().Impressions` is empty.
- Notification arrives before transaction KV write.
- Transaction KV has expired after 3 hours.
- Duplicate notification retries.
- Dedupe `SET NX EX` succeeds but Kafka write fails.
- KVDB read/write timeout.
- KVDB endpoint is down at startup.
- Unknown notification type string.
- `LoadAdNotification` and `AdmobImpTrackerNotification`: include only if they have normal event, impression, and type fields and product confirms they are part of HB notification wide output. Otherwise skip with a metric.

## 14. Review, Commit, and PR

After coding and tests:

1. Invoke `skill: review-council` on the full diff.
2. Fix every comment at mediation level or higher.
3. Run the review again.
4. Repeat until there are no mediation-or-higher comments, with a maximum of 5 review iterations.
5. If `skill: review-council` is unavailable in the environment, do not silently skip it. Record that blocker in the final implementation notes and PR body.
6. Commit all intended changes only:

```sh
git status -sb
git add <only files changed for this feature>
git commit -m "Add HB wide attribution shadow pipeline"
```

7. Push the branch and open a PR against the repository default branch.
8. The PR body must include:
   - Summary of Jaeger writer changes.
   - Summary of Scrat lookup and wide topic changes.
   - KVDB endpoint and TTL.
   - Dedupe key behavior.
   - Metrics added.
   - Tests and regression commands run.
   - `review-council` iteration count and result.
