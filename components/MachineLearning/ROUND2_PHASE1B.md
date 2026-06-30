# Round 2 — Phase 1B: Feature Gap via Trino

**Input:** high-error segment definitions + event_ids from Phase 1A  
**Goal:** Within each segment, find candidate new features that split residuals — without loading any large Spark dataset.

**Trino table:** `raw.coba2.ex_jaeger_transaction`  
**Fields of interest:** `placement_serve_results` / `placements` (nested struct — schema TBD, pending Trino re-auth)  
**Join key:** `event_id`

---

## Workflow

```
1. Take segment conditions from Phase 1A leaf (e.g. country=US, placement_type=rewarded)
2. Write Trino SQL:
   - Filter ex_jaeger_transaction by segment conditions + date range
   - Sample ~50K event_ids
   - Unnest placement_serve_results / placements struct
   - Pull candidate new columns
3. Correlate each candidate column with abs_error (join on event_id from val_predictions)
4. Rank candidates by Pearson r or mutual information with residual
```

---

## SQL Sketch

```sql
SELECT
    t.event_id,
    t.placements[1].candidate_field_a,
    t.placements[1].candidate_field_b,
    -- add more candidate columns once schema confirmed
    v.abs_error,
    v.residual
FROM raw.coba2.ex_jaeger_transaction t
JOIN (
    -- val event_ids for this segment, uploaded or filtered inline
    SELECT event_id, abs_error, residual
    FROM val_segment_events
) v ON t.event_id = v.event_id
WHERE t.dt BETWEEN '2026-06-20' AND '2026-06-27'
  AND <segment_conditions>    -- from Phase 1A leaf definition
LIMIT 50000
```

**Open item:** confirm exact column names inside `placement_serve_results`/`placements`
by re-authenticating Trino (`/mcp` → "claude.ai MX Trino Beta" → re-auth).

---

## Analysis

```python
# After pulling Trino result into pandas df:
candidate_cols = [c for c in trino_df.columns
                  if c not in ("event_id", "abs_error", "residual")]
correlations = {col: trino_df[col].corr(trino_df["abs_error"])
                for col in candidate_cols}
print(pd.Series(correlations).abs().sort_values(ascending=False))
```

---

## Checklist

- [ ] Trino `raw.coba2.ex_jaeger_transaction` schema confirmed (`placement_serve_results`/`placements`)
- [ ] SQL run per high-error segment
- [ ] Candidate features ranked by correlation with abs_error
