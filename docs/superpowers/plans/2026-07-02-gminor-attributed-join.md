# GMinor Attributed Join — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `gminor_attributed_join` SparkMain job that attaches point-in-time device + context aggregate features (and wide-table labels) to event-grain GMinor samples, producing `ml_shadow.gminor_attributed_training`.

**Architecture:** GMinor (coba2 landing, event-id sampled) `LEFT JOIN` the wide table by `event_id` (bridge) to derive the two surrogate keys with the *same* `agg_specs/*.json` expressions the aggregation job uses; then strict prior-hour point-in-time `LEFT JOIN` to both aggregate tables; write one training row per sample. Spec-driven keys guarantee equality with the agg tables by construction.

**Tech Stack:** Scala/Spark (lena `BoilerplateSparkMain`, `UDFUtil`, json4s), Iceberg on `hive_stg`, Python 3 codegen (`components/Data/codegen`, pytest).

## Global Constraints

- Namespace is `hive_stg.ml_shadow` (single namespace; not `ml_shadow_feature`).
- Output feature columns are source-prefixed: `lbl_` (wide labels), `dl_` (device_level_v1 aggregates), `ndc_` (non_device_context_v1 aggregates). GMinor fields keep their names.
- Sampling everywhere is `in_user_sample(sha1(event_id), <rate>)`, default `0.0001`, same deterministic event-id cohort as the ingestion/wide jobs.
- Point-in-time predicate is **strict prior hour**: `agg.event_time < g.event_hour` where `event_hour = date_trunc('hour', source_event_time)`. Latest snapshot via `row_number() … ORDER BY agg.event_time DESC = 1`. Bounded by `agg.lookback.hours` (default 168).
- Join keys `device_dim_id` / `context_dim_id` are built from the wide row using the identical `agg_specs/*.json` recipe as `realtime_attributed_aggregation/SparkMain.scala` — never re-derive them ad hoc.
- Schema `.md` contracts: add **notes**, never rewrite original contract lines (memory: `schema-md-edit-style`).
- SignalPrism has **no Scala build/test harness**. Scala tasks are verified by promoting to lena, building the jar (GH `build-jar.yaml`), and a spark smoke run — the loop already used for the other jobs. Only the Python codegen has pytest.

---

### Task 1: Codegen — output contract for `gminor_attributed_training`

Generates the wide output DDL/template/column-list from the two agg DDLs + a curated label set, so the ~270-column table isn't hand-maintained.

**Files:**
- Create: `components/Data/codegen/gminor_schema_catalog.py`
- Create: `components/Data/codegen/gminor_generate.py`
- Create: `components/Data/codegen/test_gminor_generate.py`
- Create (generated): `components/Data/ddl/gminor_attributed_training.sql`, `components/Data/src/main/resources/sql/gminor_attributed_training.template`, `components/Data/src/main/resources/columns/gminor_attributed_training.json`

**Interfaces:**
- Produces: `gminor_schema_catalog.GMINOR_BASE_COLS: list[(name,type)]`, `LABEL_COLS: list[str]` (wide columns surfaced as `lbl_*`), `gminor_generate.build_output_columns() -> list[(name,type)]`, `gminor_generate.write_all()`.

- [ ] **Step 1: Write the failing test**

```python
# components/Data/codegen/test_gminor_generate.py
import gminor_generate as g

def test_output_has_gminor_base_keys_labels_and_prefixed_aggs():
    cols = dict(g.build_output_columns())
    # gminor base + derived
    for c in ["event_id","project_name","experiment_id","source_event_time","event_hour",
              "sample_weight","device_id","lo_id","device_dim_id","context_dim_id"]:
        assert c in cols, f"missing {c}"
    # labels prefixed lbl_
    assert "lbl_jgr_settlement_price" in cols
    assert "lbl_jgr_no_serv_reason" in cols
    # every device_level metric appears once, prefixed dl_
    assert "dl_min_bid_to_win_sum" in cols and "dl_delivery_count" in cols
    assert "dl_agg_hit" in cols and cols["dl_agg_hit"] == "boolean"
    assert "dl_agg_event_time" in cols and cols["dl_agg_event_time"] == "timestamp"
    # every non_device metric appears once, prefixed ndc_
    assert "ndc_min_bid_to_win_sum" in cols
    assert "ndc_agg_hit" in cols and "ndc_agg_event_time" in cols
    # no raw (unprefixed) agg metric leaked
    assert "min_bid_to_win_sum" not in cols

def test_no_duplicate_columns():
    names = [n for n, _ in g.build_output_columns()]
    assert len(names) == len(set(names)), "duplicate output columns"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd components/Data/codegen && python3 -m pytest test_gminor_generate.py -q`
Expected: FAIL (`ModuleNotFoundError: gminor_generate`).

- [ ] **Step 3: Write `gminor_schema_catalog.py`**

```python
# components/Data/codegen/gminor_schema_catalog.py
# GMinor base columns (from schemas/gminor_log_schema.md §2 + §3 derived), plus the two keys.
GMINOR_BASE_COLS = [
    ("event_id", "string"), ("project_name", "string"), ("experiment_id", "bigint"),
    ("project_experiment_key", "string"),
    ("source_event_time", "timestamp"), ("event_hour", "timestamp"),
    ("traffic_allocation", "double"), ("downsampling_rate", "double"), ("sample_weight", "double"),
    ("feature_schema_version", "bigint"), ("version", "string"), ("cloud_provider", "string"),
    ("device_id", "string"), ("lo_id", "string"),
    ("features", "string"), ("predictions", "string"),
    ("device_dim_id", "string"), ("context_dim_id", "string"),
]

# Wide-table label/outcome columns to surface (schema §7 leak_risk/label-adjacent set). Surfaced as lbl_*.
LABEL_COLS = [
    "jgr_settlement_price", "jgr_settlement_status", "jgr_winning_bid_price",
    "jgr_min_bid_to_win", "jgr_second_place_price", "jgr_no_serv_reason",
    "jgr_winner_predicted_nr", "jgr_vungle_price", "jgr_is_winner_acc",
]

WIDE_DDL = "../ddl/realtime_attributed_event_wide.sql"
DEVICE_DDL = "../ddl/realtime_attributed_device_level_hly.sql"
CONTEXT_DDL = "../ddl/realtime_attributed_non_device_context_hly.sql"

# Aggregate rows carry these non-metric columns; exclude from the prefixed metric copy.
AGG_NON_METRIC = {
    "event_time", "ingest_time", "hashid", "device_dim_id", "context_dim_id", "device_id",
    "source_event_count", "first_source_event_time", "last_source_event_time", "aggregation_version",
}
```

- [ ] **Step 4: Write `gminor_generate.py`**

```python
# components/Data/codegen/gminor_generate.py
import json, os
from gminor_schema_catalog import (GMINOR_BASE_COLS, LABEL_COLS, WIDE_DDL, DEVICE_DDL,
                                    CONTEXT_DDL, AGG_NON_METRIC)

HERE = os.path.dirname(__file__)
RES = os.path.join(HERE, "..", "src", "main", "resources")
DDL_DIR = os.path.join(HERE, "..", "ddl")
OUT = "gminor_attributed_training"
TABLE = "hive_stg.ml_shadow." + OUT
LOCATION = "s3a://vungle2-dataeng/ml_shadow/" + OUT

def _ddl_cols(path):
    body = open(os.path.join(HERE, path)).read()
    body = body[body.index("(") + 1: body.rindex(")")]
    out = []
    for line in body.splitlines():
        line = line.strip().rstrip(",")
        if not line or line.split()[0] in ("USING","PARTITIONED","LOCATION","TBLPROPERTIES","'sort-order'"):
            continue
        parts = line.split(None, 1)
        if len(parts) == 2:
            out.append((parts[0], parts[1].lower()))
    return out

def _wide_type(name):
    for n, t in _ddl_cols(WIDE_DDL):
        if n == name:
            return t
    raise KeyError(name)

def _agg_metrics(path):
    return [(n, t) for n, t in _ddl_cols(path) if n not in AGG_NON_METRIC]

def build_output_columns():
    cols = list(GMINOR_BASE_COLS)
    cols.append(("wide_join_hit", "boolean"))
    for c in LABEL_COLS:
        cols.append(("lbl_" + c, _wide_type(c)))
    for n, t in _agg_metrics(DEVICE_DDL):
        cols.append(("dl_" + n, t))
    cols += [("dl_agg_hit", "boolean"), ("dl_agg_event_time", "timestamp")]
    for n, t in _agg_metrics(CONTEXT_DDL):
        cols.append(("ndc_" + n, t))
    cols += [("ndc_agg_hit", "boolean"), ("ndc_agg_event_time", "timestamp")]
    return cols

def _render_ddl(table, location):
    body = ",\n".join("    %s %s" % (n, t) for n, t in build_output_columns())
    return ("CREATE TABLE IF NOT EXISTS %s (\n" % table + body + "\n)\n"
            "USING iceberg\n"
            "PARTITIONED BY (hours(source_event_time))\n"
            'LOCATION "%s"\n'
            "TBLPROPERTIES (\n  'sort-order' = 'source_event_time ASC NULLS FIRST, event_id ASC NULLS FIRST'\n)\n"
            % location)

def _write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    open(path, "w").write(text)

def write_all():
    _write(os.path.join(DDL_DIR, OUT + ".sql"), _render_ddl(TABLE, LOCATION))
    _write(os.path.join(RES, "sql", OUT + ".template"), _render_ddl("?table?", "?location?"))
    _write(os.path.join(RES, "columns", OUT + ".json"),
           json.dumps([n for n, _ in build_output_columns()], indent=2) + "\n")

if __name__ == "__main__":
    write_all(); print("gminor: ddl + template + columns written")
```

- [ ] **Step 5: Run tests + generate**

Run: `cd components/Data/codegen && python3 -m pytest test_gminor_generate.py -q && python3 gminor_generate.py`
Expected: PASS (2 tests), then `gminor: ddl + template + columns written`. Confirm `../ddl/gminor_attributed_training.sql` exists and begins `CREATE TABLE IF NOT EXISTS hive_stg.ml_shadow.gminor_attributed_training`.

- [ ] **Step 6: Commit**

```bash
git add components/Data/codegen/gminor_schema_catalog.py components/Data/codegen/gminor_generate.py \
        components/Data/codegen/test_gminor_generate.py components/Data/ddl/gminor_attributed_training.sql \
        components/Data/src/main/resources/sql/gminor_attributed_training.template \
        components/Data/src/main/resources/columns/gminor_attributed_training.json
git commit -m "feat(codegen): gminor_attributed_training output contract (ddl/template/columns)"
```

---

### Task 2: Shared spec-driven key helper (Scala)

Extract the spec→expression logic from the aggregation job into a shared object so both jobs build identical keys.

**Files:**
- Create: `components/Data/src/main/scala/com/vungle/signalprism/data/agg/KeySpec.scala`
- Modify: `components/Data/src/main/scala/com/vungle/signalprism/data/realtime_attributed_aggregation/SparkMain.scala` (replace inline `parseObj/arr/obj/str/dimExpr/keyConcatArg/spec/dims/primaryKey/dropNullSrc` with calls to `KeySpec`)

**Interfaces:**
- Produces:
  - `KeySpec.dimProjections(family: String): Seq[String]` — `"<dimExpr> AS <name>"` per non-surrogate dim.
  - `KeySpec.allDimProjections(families: Seq[String]): Seq[String]` — union of dim projections deduped by output name (identical names have identical exprs; asserts if not).
  - `KeySpec.surrogateExpr(family: String): String` — `"sha2(concat_ws('|', coalesce(CAST(<name> AS STRING),'__unknown__'), …), 256)"` referencing that family's dim names in spec order.
  - `KeySpec.primaryKeyName(family)`, `KeySpec.dropNullSource(family)`, `KeySpec.dims(family)`.

- [ ] **Step 1: Create `KeySpec.scala`** (move the exact logic; `dimExpr`/`keyConcatArg` copied verbatim from the agg job so behavior is identical)

```scala
package com.vungle.signalprism.data.agg

import com.vungle.lena.BoilerplateSparkMain
import org.json4s.{JValue, JObject, JArray, JString, JNull, JNothing}
import org.json4s.native.JsonMethods.parse

/** Single source of truth for spec-driven aggregation dimension + surrogate-key SQL.
 *  Used by realtime_attributed_aggregation and gminor_attributed_join so keys can't drift. */
object KeySpec {
  private def readResource(res: String): String =
    scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(res), "UTF-8").mkString
  private def parseObj(res: String): Map[String, JValue] =
    parse(readResource(res)).asInstanceOf[JObject].obj.toMap
  private def arr(a: JValue): List[JValue] = a.asInstanceOf[JArray].arr
  private def obj(a: JValue): Map[String, JValue] = a.asInstanceOf[JObject].obj.toMap
  private def str(a: JValue): String = a match {
    case JString(s) => s; case JNull | JNothing => null; case other => other.values.toString
  }

  private def spec(family: String) = parseObj(s"agg_specs/$family.json")
  def dims(family: String): List[Map[String, JValue]] = arr(spec(family)("dimensions")).map(obj)
  def primaryKeyName(family: String): String = str(obj(spec(family)("primary_key"))("name"))
  def dropNullSource(family: String): String = str(spec(family).getOrElse("drop_null_source", JNull))

  private def dimExpr(d: Map[String, JValue]): String = {
    val name = str(d("name")); val src = str(d("source_col")); val norm = str(d("norm"))
    val e = norm match {
      case _ if src == null       => "CAST(NULL AS STRING)"
      case "parse_major"          => s"split($src, '\\\\.')[0]"
      case "coalesce"             => s"coalesce($src, ${str(d("fallback_col"))})"
      case "normalize" | "bucket" => s"lower(trim($src))"
      case "expr"                 => src
      case _                      => src
    }
    s"$e AS $name"
  }
  private def keyConcatArg(d: Map[String, JValue]): String =
    s"coalesce(CAST(${str(d("name"))} AS STRING), '__unknown__')"

  def dimProjections(family: String): Seq[String] = dims(family).map(dimExpr)

  def allDimProjections(families: Seq[String]): Seq[String] = {
    val byName = scala.collection.mutable.LinkedHashMap[String, String]()
    for (f <- families; d <- dims(f)) {
      val name = str(d("name")); val proj = dimExpr(d)
      byName.get(name).foreach(p => require(p == proj, s"dim '$name' has conflicting exprs across families"))
      byName(name) = proj
    }
    byName.values.toSeq
  }

  def surrogateExpr(family: String): String =
    s"sha2(concat_ws('|', ${dims(family).map(keyConcatArg).mkString(", ")}), 256)"
}
```

- [ ] **Step 2: Refactor the aggregation SparkMain to use `KeySpec`**

In `realtime_attributed_aggregation/SparkMain.scala`: delete the private `parseObj/arr/obj/str` helpers, `dimExpr`, `keyConcatArg`, and the `spec/primaryKey/dropNullSrc/dims` lazy vals; replace usages:
- `dims.map(dimExpr)` → `KeySpec.dimProjections(family)`
- `dims.map(d => str(d("name")))` → keep a local `dimNames = KeySpec.dims(family).map(d => KeySpec-exposed name)` — add `KeySpec.dimNames(family): Seq[String]` returning `dims(family).map(str(_("name")))` and use it.
- `dims.map(keyConcatArg)` surrogate → `KeySpec.surrogateExpr(family)` (drop the local concat).
- `primaryKey` → `KeySpec.primaryKeyName(family)`; `dropNullSrc` → `KeySpec.dropNullSource(family)`.
- `metricCat` stays local (metric catalog is not key logic).

Add to `KeySpec`: `def dimNames(family: String): Seq[String] = dims(family).map(str(_("name")))` and expose `str` need — reuse internal.

- [ ] **Step 3: Verify behavior unchanged (build + parity)**

No SignalPrism Scala tests exist. Verify by promotion:
Run (after Task 6 sync): rebuild lena jar, re-run the device_level aggregation for `2026-06-28 00:00`, and confirm a known `device_dim_id` is unchanged vs the current table (spot-check one row via the S3 parquet). Expected: identical `device_dim_id` values → refactor is behavior-preserving.

- [ ] **Step 4: Commit**

```bash
git add components/Data/src/main/scala/com/vungle/signalprism/data/agg/KeySpec.scala \
        components/Data/src/main/scala/com/vungle/signalprism/data/realtime_attributed_aggregation/SparkMain.scala
git commit -m "refactor(agg): extract spec-driven KeySpec shared by aggregation + gminor jobs"
```

---

### Task 3: `gminor_attributed_join` SparkMain

**Files:**
- Create: `components/Data/src/main/scala/com/vungle/signalprism/data/gminor_attributed_join/SparkMain.scala`

**Interfaces:**
- Consumes: `KeySpec.allDimProjections`, `KeySpec.surrogateExpr` (Task 2); `columns/gminor_attributed_training.json` is the output contract (Task 1); `UDFUtil.registerCommonUDF` (provides `normalize_device_id`, `in_user_sample`); boilerplate `withCoba2TempViewInRange`, `appendToIcebergTable`, `getTillTime`, `hourlyPeriodsForCurrentBatchMinute`, `createTestTable`, `assertTestTableName`.

- [ ] **Step 1: Write the job**

```scala
package com.vungle.signalprism.data.gminor_attributed_join

import com.vungle.lena.{BoilerplateSparkMain, UDFUtil}
import com.vungle.signalprism.data.agg.KeySpec
import com.vungle.signalprism.data.gminor_attributed_join.GminorConfig._   // label list mirror (below)
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/** GMinor prediction samples -> point-in-time device + context aggregate features + wide labels.
 *  Wide table is a bridge for the two surrogate keys (KeySpec) and labels; features come from the
 *  two aggregate tables. Contract: schemas/gminor_log_schema.md §4, MLOps TRD §7.9. */
object SparkMain extends BoilerplateSparkMain {
  private val NS = "spark.app.signalprism.data.gminor_attributed_join"

  def requiredArgs: List[String] = List(
    "spark.app.env", "spark.app.batch_jobs.db.url",
    s"$NS.input.gminor.s3Dir", s"$NS.input.gminor.topic",
    s"$NS.input.wide.tableName", s"$NS.input.device_agg.tableName",
    s"$NS.input.context_agg.tableName", s"$NS.output.tableName")

  def defaultArgs: Map[String, String] = Map(
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.rdd.compress" -> "true",
    s"$NS.sample_rate" -> "0.0001",
    s"$NS.agg.lookback.hours" -> "168",
    s"$NS.test.create.table" -> "false",
    s"$NS.output.s3Dir" -> "",
    "spark.hadoop.fs.s3a.fast.upload" -> "true")

  lazy val gminorS3   = args(s"$NS.input.gminor.s3Dir")
  lazy val gminorTopic= args(s"$NS.input.gminor.topic")
  lazy val wideTable  = args(s"$NS.input.wide.tableName")
  lazy val deviceAgg  = args(s"$NS.input.device_agg.tableName")
  lazy val contextAgg = args(s"$NS.input.context_agg.tableName")
  lazy val outputTable= args(s"$NS.output.tableName")
  lazy val outputS3   = args(s"$NS.output.s3Dir")
  lazy val sampleRate = args(s"$NS.sample_rate").toDouble
  lazy val lookbackH  = args(s"$NS.agg.lookback.hours").toInt
  lazy val testCreate = args(s"$NS.test.create.table").toBoolean

  // GMinor timestamp: documented 'yyyy-MM-dd HH:mm:ss.SSS'; also RFC3339/Nano. coalesce parsers.
  private val TS =
    "coalesce(to_timestamp(`timestamp`,'yyyy-MM-dd HH:mm:ss.SSS'), " +
    "to_timestamp(`timestamp`), " +
    "to_timestamp(regexp_replace(`timestamp`,'(\\\\.[0-9]{6})[0-9]*Z$','$1Z'),\"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\"))"

  // scalastyle:off
  def process(next: DateTime, tempTable: String): Unit = {
    val startMillis = System.currentTimeMillis()
    val s = next.minusHours(1).toString("yyyy-MM-dd HH:mm:ss")   // window handled by withCoba2TempViewInRange
    val t = next.toString("yyyy-MM-dd HH:mm:ss")

    // 1) sampled gminor, event grain, parsed time
    spark.sql(s"""
      SELECT event_id, project_name, experiment_id,
             concat(project_name, ':', cast(experiment_id AS string)) AS project_experiment_key,
             $TS AS source_event_time,
             date_trunc('HOUR', $TS) AS event_hour,
             traffic_allocation, downsampling_rate,
             1.0 / nullif(downsampling_rate, 0.0) AS sample_weight,
             feature_schema_version, version, cloud_provider,
             device_id, lo_id, features, predictions
        FROM $tempTable
       WHERE in_user_sample(sha1(event_id), $sampleRate) AND event_id IS NOT NULL
    """).createOrReplaceTempView("_gminor")

    // 2) wide bridge -> both surrogate keys (KeySpec, same as agg) + labels, event grain
    val dimProj = KeySpec.allDimProjections(Seq("device_level_v1", "non_device_context_v1")).mkString(",\n        ")
    val labelSel = LABEL_COLS.map(c => s"$c AS lbl_$c").mkString(",\n        ")
    spark.sql(s"""
      SELECT event_id,
             ${KeySpec.surrogateExpr("device_level_v1")}       AS device_dim_id,
             ${KeySpec.surrogateExpr("non_device_context_v1")} AS context_dim_id,
             $labelSel
        FROM (
          SELECT event_id,
                 $dimProj,
                 ${LABEL_COLS.mkString(", ")}
            FROM $wideTable
           WHERE source_event_time >= '$s' AND source_event_time < '$t'
        )
    """).createOrReplaceTempView("_wide_keyed")

    val labelJoinSel = LABEL_COLS.map(c => s"w.lbl_$c").mkString(", ")
    spark.sql(s"""
      SELECT g.*, w.device_dim_id, w.context_dim_id, (w.event_id IS NOT NULL) AS wide_join_hit,
             $labelJoinSel
        FROM _gminor g LEFT JOIN _wide_keyed w ON g.event_id = w.event_id
    """).createOrReplaceTempView("_keyed")

    // 3) point-in-time device + context joins (strict prior hour, latest, lookback-bounded)
    def pit(view: String, aggTable: String, keyCol: String, prefix: String): Unit = {
      val metricCols = getColsInJson(s"columns/gminor_attributed_training.json")
        .filter(_.startsWith(prefix)).filterNot(Set(s"${prefix}agg_hit", s"${prefix}agg_event_time"))
        .map(c => s"a.${c.substring(prefix.length)} AS $c").mkString(",\n         ")
      spark.sql(s"""
        SELECT k.*, $metricCols,
               a.event_time AS ${prefix}agg_event_time,
               (a.$keyCol IS NOT NULL) AS ${prefix}agg_hit
          FROM (SELECT *,
                       row_number() OVER (PARTITION BY event_id ORDER BY a_event_time DESC) AS _rn
                  FROM (
                    SELECT k.*, a.event_time AS a_event_time
                      FROM $view k
                      LEFT JOIN $aggTable a
                        ON a.$keyCol = k.$keyCol
                       AND a.event_time <  k.event_hour
                       AND a.event_time >= k.event_hour - INTERVAL '$lookbackH' HOUR
                  )) k
          LEFT JOIN $aggTable a
            ON a.$keyCol = k.$keyCol AND a.event_time = k.a_event_time
         WHERE _rn = 1
      """).drop("_rn", "a_event_time").createOrReplaceTempView(view + "_pit")
    }
    // NOTE: implement the two PIT joins as two CTEs in one statement per aggregate; the helper above
    // shows the shape. Device first, then context on its output. Keep row_number per event_id.
    // (Concrete two-stage SQL is written inline; see Step-2 refinement.)

    val out = spark.table("_keyed")  // replaced by the final PIT-joined DataFrame
    appendToIcebergTable(outputTable, out)
    reportStatsMetric(s"$appName.write.seconds", (System.currentTimeMillis() - startMillis) / 1000)
  }
  // scalastyle:on

  def run: Unit = {
    UDFUtil.registerCommonUDF(spark)
    UDFUtil.registerInUserSample(spark)
    assertTestTableName(outputTable)
    if (isTest && testCreate) createTestTable(outputTable, outputS3, "sql/gminor_attributed_training.template")

    val pattern = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val finalTill = DateTime.parse(getTillTime(s"$NS.till", "yyyy-MM-dd HH:mm:ss"), pattern)
    val tillDay = finalTill.toString("yyyy-MM-dd"); val tillHour = finalTill.toString("HH")
    val tillMinute = finalTill.toString("mm")
    for ((nextStart, nextTill) <- hourlyPeriodsForCurrentBatchMinute(tillDay, tillHour, tillMinute)) {
      assert(isTimeBeforeNow(nextTill))
      withCoba2TempViewInRange(gminorS3, gminorTopic, nextStart, nextTill, "_gminor_src") {
        case (tempTable, _) => process(nextTill, tempTable)
      }
      if (isNotBackfill) recordProgressMinute(nextTill)
    }
    reportStatsMetric(s"$appName.success", 1)
  }
}
```

- [ ] **Step 2: Add the config mirror + finalize the PIT SQL**

Create `GminorConfig` object in the same file holding `val LABEL_COLS = Seq(...)` mirroring `gminor_schema_catalog.LABEL_COLS` (Scala can't read the Python list; keep both in sync — add a comment cross-referencing). Replace the sketch `pit(...)`/`out` with the concrete two-stage point-in-time SQL: for each aggregate build a CTE that LEFT JOINs the agg on `keyCol` with `a.event_time < k.event_hour AND a.event_time >= k.event_hour - INTERVAL 'lookback' HOUR`, applies `row_number() OVER (PARTITION BY event_id ORDER BY a.event_time DESC)`, keeps `=1`, and prefixes surviving agg metric columns with `dl_`/`ndc_` (metric names read from `columns/gminor_attributed_training.json`). Chain device→context; final DataFrame = context stage. `appendToIcebergTable` null-fills any output columns not selected.

- [ ] **Step 3: Verify (build + smoke, via lena)**

After Task 6 sync + jar build, run the backfill (Task 5) for a target hour that has prior agg hours available. Expected driver log: `Append data to hive_stg.ml_shadow.gminor_attributed_training … with N records!` where N = sampled GMinor rows for the hour. Then verify (Trino can't see hive_stg; use spark-sql or S3 parquet): `wide_join_hit`, `dl_agg_hit`, `ndc_agg_hit` fractions > 0; no row with `dl_agg_event_time >= event_hour`.

- [ ] **Step 4: Commit**

```bash
git add components/Data/src/main/scala/com/vungle/signalprism/data/gminor_attributed_join/SparkMain.scala
git commit -m "feat(data): gminor_attributed_join point-in-time feature join job"
```

---

### Task 4: Annotate `schemas/gminor_log_schema.md` (notes only)

**Files:**
- Modify: `schemas/gminor_log_schema.md` (add note blocks; do not change existing lines)

- [ ] **Step 1: Add a device-key note after §4.2**

Insert after the §4.2 code block (do not edit the existing SQL):

```markdown
> **Note (device key updated 2026-07-02):** the aggregate `device_level_v1.device_id` is now
> `normalize_device_id(jgr_dev_normalized_id)` (not `jgr_lo_id`, which is empty upstream). The
> `gminor_attributed_join` job therefore joins on **`device_dim_id`** derived from the wide bridge
> using the same `agg_specs` recipe as the aggregation job — GMinor's own `device_id`/`lo_id` are
> carried as columns, not used as the join key. See design spec 2026-07-02-gminor-attributed-join.
```

- [ ] **Step 2: Add a point-in-time note at §5**

Append a bullet under §5 (do not edit existing bullets):

```markdown
- **Point-in-time uses strict prior hour:** the implementation joins with
  `aggregate.event_time < date_trunc('hour', source_event_time)` (prior hour only), which supersedes
  the `< source_event_time` shown in the §4.2/§4.3 inline examples (that would admit the same hour,
  whose aggregate can contain at/after-event data).
```

- [ ] **Step 3: Commit**

```bash
git add schemas/gminor_log_schema.md
git commit -m "docs(schema): note gminor device key = device_dim_id + strict prior-hour PIT"
```

---

### Task 5: lena CD backfill YAML + agg prerequisite

**Files:**
- Create: `/Users/twang/Projects/lena/cd/lena-test/stage-signal-prism-gminor-join-backfill.yaml`

- [ ] **Step 1: Author the YAML** by copying `stage-signal-prism-agg-non-device-context-backfill.yaml` and changing:
  - `metadata.name` → `sp-gminor-join-bf` (short — avoids the 63-char label limit),
  - `mainClass` → `com.vungle.signalprism.data.gminor_attributed_join.SparkMain`,
  - `podNamePrefix`/upload path → `sp-gminor-join-bf`,
  - arg keys under `$NS = ...gminor_attributed_join`: `input.gminor.s3Dir: "s3a://vungle2-logs/coba2/data"`, `input.gminor.topic: "ex-gminor-logs"`, `input.wide.tableName`, `input.device_agg.tableName`, `input.context_agg.tableName`, `output.tableName: "hive_stg.ml_shadow.gminor_attributed_training"`, `output.s3Dir`, `sample_rate: "0.0001"`, `agg.lookback.hours: "168"`, `till`.
  - Set `till` to a target hour that is **after** the backfilled agg hours (see Step 2).

- [ ] **Step 2: Document the multi-hour agg prerequisite** in a YAML comment header: point-in-time features are strictly prior-hour, so before running this job you must backfill `device_level_hly` and `non_device_context_hly` for a range of hours (e.g. `2026-06-28 00:00–06:00`) and set this job's `till` to a later hour (e.g. `2026-06-28 07:00`); otherwise all `dl_/ndc_` features are null.

- [ ] **Step 3: Commit (lena repo)**

```bash
git -C /Users/twang/Projects/lena add cd/lena-test/stage-signal-prism-gminor-join-backfill.yaml
git -C /Users/twang/Projects/lena commit -m "cd: gminor-join backfill SparkApplication (stage)"
```

---

### Task 6: Sync to lena, build, smoke-run

**Files:** (lena mirrors of Task 1–3 outputs)

- [ ] **Step 1: Sync resources + scala into lena**

```bash
cd /Users/twang/Projects
cp SignalPrism/components/Data/src/main/resources/sql/gminor_attributed_training.template lena/src/main/resources/sql/
cp SignalPrism/components/Data/src/main/resources/columns/gminor_attributed_training.json lena/src/main/resources/columns/
mkdir -p lena/src/main/scala/com/vungle/signalprism/data/agg lena/src/main/scala/com/vungle/signalprism/data/gminor_attributed_join
cp SignalPrism/components/Data/src/main/scala/com/vungle/signalprism/data/agg/KeySpec.scala lena/src/main/scala/com/vungle/signalprism/data/agg/
cp SignalPrism/components/Data/src/main/scala/com/vungle/signalprism/data/gminor_attributed_join/SparkMain.scala lena/src/main/scala/com/vungle/signalprism/data/gminor_attributed_join/
cp SignalPrism/components/Data/src/main/scala/com/vungle/signalprism/data/realtime_attributed_aggregation/SparkMain.scala lena/src/main/scala/com/vungle/signalprism/data/realtime_attributed_aggregation/
diff -rq SignalPrism/components/Data/src/main/scala/com/vungle/signalprism lena/src/main/scala/com/vungle/signalprism
```
Expected: no diffs.

- [ ] **Step 2: Create the output table** — run `components/Data/ddl/gminor_attributed_training.sql` against `hive_stg` (spark-sql).

- [ ] **Step 3: Backfill agg prerequisite hours** — run device + non-device aggregation for `2026-06-28 00:00–06:00` (existing YAMLs, loop the `till`).

- [ ] **Step 4: Build jar + run** — commit lena, build via `.github/workflows/build-jar.yaml`, point the Task-5 YAML `mainApplicationFile` at the new `lena-<ref>.jar`, set `till: "2026-06-28 07:00:00"`, run.

- [ ] **Step 5: Verify** — driver log shows non-zero append; spot-check hit fractions and PIT correctness per Task 3 Step 3. Commit any YAML jar-ref bump in lena.

---

## Notes for the implementer

- The two aggregation jobs and this job share `KeySpec`; if you change any `agg_specs/*.json`, both keys move together — re-verify parity (Task 2 Step 3).
- `LABEL_COLS` exists twice (Python `gminor_schema_catalog.py` and Scala `GminorConfig`); keep them identical (cross-referenced by comment). If this becomes a burden, generate the Scala list from the Python one in `gminor_generate.py`.
- Inequality (point-in-time) joins are the expensive step; at `sample_rate=0.0001` over one hour the volume is small. If raising the sample rate, watch the agg-side scan — the `lookback` bound and `event_time` partition pruning keep it tractable.
