# WideTable KVDB Resource Estimation (Realtime HB Attribution)

**Date:** 2026-07-01
**Branch under analysis:** `wip-ai-workshop-signal-prism` (Jaeger monorepo) — the HB wide‑attribution shadow pipeline
**KVDB endpoint:** `prod-agg-device-feature.rocksdb.vungle.io:6379` — a **KVRocks / RocksDB (disk‑backed)** store
**Goal:** size the KVDB (storage, key count, write bandwidth, ops/s) as a function of the key **TTL granularity**.

---

## 1. TL;DR

- Storage, key count, and write bandwidth are all **linear in TTL**. Going from the current **3h** TTL to **7d** is a **56×** increase.
- At the current **3h** TTL, storing the full transaction for the HB‑transaction stream needs on the order of **~11–12 TB** of KVRocks logical storage and **~3.6 billion live keys** (or **~25 TB / ~4.5 B keys** using the looser `ex-jaeger-transaction` proxy). A **7‑day** TTL would need **~0.6–1.4 PB** and **~200–250 billion keys**.
- The **binding constraint is very likely write throughput + RocksDB compaction, not capacity**: ~1–2.3 GB/s of logical writes at ~330–415 K SET/s, amplified ~10–30× by leveled compaction ⇒ ~10–70 GB/s of physical disk writes. This must be **sharded across many nodes**; a single instance cannot sustain it.
- The spec deliberately fixes TTL at **3h (10800 s)** and explicitly lists **"do not implement a 7‑day TTL"** as out of scope — this estimate shows why: the 7‑day footprint is ~2 orders of magnitude larger.
- The biggest lever to shrink the footprint is **value size**: store a *projection* (only join/debug fields, ~a few hundred bytes) instead of the full ~3 KB `hbtransaction.Message` → ~5–10× reduction in both storage and write bandwidth.

---

## 2. What the branch writes to the KVDB

From the implementation (`internal/wideattribution`, `internal/wideattribution/writer`, `internal/scrat/hbnotificationwide`):

| Key | Written by | Value | TTL |
|---|---|---|---|
| `wt:hbtxn:v1:<esc event_id>:<esc imp_id>` | Jaeger async writer, on the **successful realtime HB delivery** gate (realtime + enriched + no‑serv‑none + non‑test + raw request present) | `easyjson(*hbtransaction.Message)` — the **full** HB transaction | `10800s` (3h) |
| `wt:hbnwide:dedupe:v1:<esc event_id>:<esc imp_id>:<esc notification_type>` | Scrat, on a lookup **hit** via `SET NX EX` | sentinel `"1"` (~1 byte) | `10800s` (3h) |

- Key parts are base64‑rawURL escaped. Transaction key length ≈ **~65–75 bytes** (`wt:hbtxn:v1:` + base64(event_id≈UUID) + base64(imp_id)); dedupe key ≈ **~80–90 bytes**.
- The store is **KVRocks (RocksDB)** → resource is primarily **disk** (with block cache in RAM), and **compaction write‑amplification** is a first‑class cost.
- **Two op classes:** Jaeger side = `SET … EX` (writes); Scrat side = `GET` + `SET NX EX` (+ rare `DEL` on wide‑write failure).

---

## 3. Measured inputs (Grafana, `dmx_prod_revenue`, 1‑hour average, 2026‑07‑01)

Source dashboard: `kafka-prod` (`5qxfXQ5Vz`).
- Panel 7 — *Message In per Topic* (`kafka_server_brokertopicmetrics_messagesin_total`): message rate.
- Panel 14 — *Topic Network In* (`kafka_server_brokertopicmetrics_bytesin_total`): byte rate.

| Topic | Msg rate | Byte rate | Avg size/msg | Role in this design |
|---|---|---|---|---|
| `ex-jaeger-transaction-20220421` | **415,382 /s** (peak ~429 K) | **2.308 GB/s** | **5.56 KB** | Headline proxy you selected (loose upper bound of transaction volume) |
| `hb-transactions-20220421` | **332,128 /s** | **1.023 GB/s** | **3.08 KB** | **Closest to what the KVDB actually stores** (`hbtransaction.Message`) |
| `hb-notifications-20220421` | **404,238 /s** | — | — | Scrat read/dedupe driver (1 `GET` each, +`SETNX` on hit) |

> **Proxy caveat.** The wide writer stores `hbtransaction.Message` (≈ the `hb-transactions` topic), and only on the **successful‑realtime‑delivery gate** — a *subset* of `hb-transactions`. So the true write rate is **≤ 332 K/s**, and `ex-jaeger-transaction` (415 K/s) is an even looser upper bound. Both are used below to bracket the estimate; the real number sits at or below the `hb-transactions` row after applying the gate‑pass fraction `g` (realtime share × delivery‑success rate).
>
> **Compression caveat.** Kafka `bytesin` is on‑the‑wire size (revenue producers typically compress). The KVDB stores raw easyjson, which RocksDB then re‑compresses on disk. Net: the kafka byte size is a reasonable proxy for **RocksDB compressed on‑disk** size; the **uncompressed working set** (memtables, block cache, un‑compacted L0) is ~1.8–3× larger. Storage figures below are "reference (kafka‑wire) bytes"; see §7 for the compression band.

---

## 4. Sizing model

Let:
- `Nw` = KVDB write rate (transaction keys/s) — `Nw = g × 332,128` (gate‑pass fraction `g ≤ 1`); upper bound 415,382.
- `Sv` = value bytes/msg (reference/compressed): 3.08 KB (hb‑transactions) or 5.56 KB (ex‑jaeger).
- `Ov` = per‑entry key + RocksDB metadata overhead ≈ **120 bytes** (key ~70 B + seq/type/index/filter amortized ~50 B).
- `T` = TTL in seconds.

Then at steady state:
- **Live transaction keys** = `Nw × T`
- **Transaction storage** = `Nw × (Sv + Ov) × T` ≈ `(byte_rate + Nw×Ov) × T`
- **Dedupe keys** ≈ `Nd × T`, `Nd ≤ 404,238/s` (notification hits), each ~140 B ⇒ a **secondary** term (~5% of transaction storage; see §6).

TTL is a pure linear multiplier, so the table below scales any row to any TTL.

---

## 5. Storage & key count vs TTL

### 5a. Basis = `hb-transactions` (what we actually store: 332,128 /s · 1.023 GB/s · 3.08 KB)

| TTL | Live txn keys | Txn storage (value + keys/overhead) |
|---|---|---|
| **3h (current)** | **3.59 B** | **≈ 11.5 TB** |
| 6h | 7.17 B | ≈ 23.0 TB |
| 12h | 14.3 B | ≈ 45.9 TB |
| 24h (1d) | 28.7 B | ≈ 91.8 TB |
| 3d | 86.1 B | ≈ 275 TB |
| 7d | 201 B | ≈ 643 TB |

### 5b. Basis = `ex-jaeger-transaction` (your headline proxy / upper bound: 415,382 /s · 2.308 GB/s · 5.56 KB)

| TTL | Live txn keys | Txn storage (value + keys/overhead) |
|---|---|---|
| **3h (current)** | **4.49 B** | **≈ 25.5 TB** |
| 6h | 8.97 B | ≈ 50.9 TB |
| 12h | 17.9 B | ≈ 101.8 TB |
| 24h (1d) | 35.9 B | ≈ 203.7 TB |
| 3d | 107.7 B | ≈ 611 TB |
| 7d | 251.2 B | ≈ 1.43 PB |

> Real deployment sits **between "gated `hb-transactions`" and these two rows**, scaled by the gate‑pass fraction `g`. Example: if only 40% of `hb-transactions` pass the successful‑realtime gate (`g=0.4`), multiply the 5a numbers by 0.4 → **3h ≈ 4.6 TB, ~1.4 B keys**.

---

## 6. Dedupe keys (Scrat side) — secondary term

Upper bound = every HB notification hits (404,238/s), ~140 B/entry:

| TTL | Dedupe keys (≤) | Dedupe storage (≤) |
|---|---|---|
| 3h | 4.37 B | ≈ 0.61 TB |
| 24h | 34.9 B | ≈ 4.9 TB |
| 7d | 244 B | ≈ 34 TB |

≈ **5% of transaction storage** and comparable key count. Realistically lower (only notifications whose transaction is still within TTL produce a dedupe key). Include as a ~5–10% add‑on to §5.

---

## 7. Compression sensitivity

The §5 figures use the kafka‑wire byte size as the reference.

- If kafka bytes are **compressed** (likely on revenue producers): §5 ≈ **RocksDB compressed on‑disk** size. Provision the **uncompressed working set** (memtable + block cache + L0) separately at ~1.8–3× the per‑key value for RAM/hot data.
- If kafka bytes are **uncompressed**: RocksDB LZ4/Zstd on JSON typically yields **3–5×**, so divide the §5 storage by ~3–4 for on‑disk (e.g., hb‑transactions 3h ⇒ ~3–4 TB on disk), but plan block cache on the uncompressed size.

**Practical band at 3h TTL (hb‑transactions basis, gate‑pass `g`):** on‑disk ≈ **`g` × (3 TB … 11.5 TB)** depending on compression, plus ~5–10% dedupe.

---

## 8. Throughput & ops/s (usually the real constraint)

These do **not** depend on TTL — they scale with the live traffic and are what a single node can/can't sustain.

**Write side (Jaeger):**
- Logical write bandwidth: **1.0 GB/s** (hb‑transactions basis) to **2.3 GB/s** (ex‑jaeger basis), × `g`.
- `SET` ops: **332 K–415 K /s**, × `g`.
- **RocksDB compaction write amplification** (leveled, ~10–30×) ⇒ **~10–70 GB/s physical disk writes**, plus continuous compaction of billions of TTL‑expiring keys. **This is the dominant cost** and forces heavy NVMe + horizontal sharding.

**Read side (Scrat):**
- `GET`: ≈ **404 K/s** (one per HB notification).
- `SET NX EX`: ≤ 404 K/s (one per lookup hit).
- `DEL`: ≈ 0 (only on wide‑Kafka‑write failure).

**Total KVDB op load ≈ 0.7 M – 1.2 M ops/s** and **1–2.3 GB/s ingest** → a **large sharded KVRocks cluster**, not a single instance. TTL changes capacity/compaction pressure but not this baseline op rate.

---

## 9. Recommendations

1. **TTL is the primary capacity knob (linear).** Keep the POC at **3h**. Each doubling of TTL doubles storage, key count, and expiry‑compaction load. 7‑day is ~56× the 3h footprint (0.6–1.4 PB, 200 B+ keys) — consistent with the spec explicitly ruling 7‑day out of scope.
2. **Shrink the value, not just the TTL.** Storing a **projection** (only the join/debug fields the wide row needs — a few hundred bytes) instead of the full ~3 KB `hbtransaction.Message` cuts storage **and** write bandwidth ~5–10× and is TTL‑independent. This is the highest‑leverage change if longer TTL is ever needed.
3. **Plan for write throughput + compaction first.** At 1–2.3 GB/s logical (×WA), size for sustained physical write bandwidth and compaction headroom across shards; capacity alone understates the requirement.
4. **Tighten the write gate** to exactly the impressions that can receive an HB notification (reduces `g`), so you don't store transactions that will never be joined.
5. **Keep dedupe keys as‑is** — they're a ~5–10% add‑on; not worth optimizing before the value size.
6. **Coordinate with PE (`#eng-supply-infra`) before any TTL increase or PROD enablement** — this is a PE‑owned KVDB and the write bandwidth/compaction load is substantial (per the jaeger `CLAUDE.md` "KVDB Operational Changes" rule and the v558 Valkey incident precedent).

---

## 10. Reproduce

```promql
# message rate (Panel 7) — substitute the topic
sum(rate(kafka_server_brokertopicmetrics_messagesin_total{kafka_cluster=~".*dmx_prod_revenue", topic="ex-jaeger-transaction-20220421"}[1h]))
# byte rate (Panel 14)
sum(rate(kafka_server_brokertopicmetrics_bytesin_total{kafka_cluster=~".*dmx_prod_revenue", topic="ex-jaeger-transaction-20220421"}[1h]))
# avg msg size = byte rate / message rate
```
Datasource: Mimir `PAE45454D0EDB9216`. Repeat with `topic="hb-transactions-20220421"` and `topic="hb-notifications-20220421"`.

**Storage(TTL) ≈ (measured byte_rate + write_rate × 120 B) × TTL_seconds × gate_fraction`g`**, then apply the §7 compression band; add ~5–10% for dedupe keys.
