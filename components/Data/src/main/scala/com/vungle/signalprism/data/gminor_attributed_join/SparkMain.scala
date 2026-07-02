package com.vungle.signalprism.data.gminor_attributed_join

import com.vungle.lena.{BoilerplateSparkMain, UDFUtil}
import com.vungle.signalprism.data.agg.KeySpec
import com.vungle.signalprism.data.gminor_attributed_join.GminorConfig._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * Wide-table label/outcome columns surfaced as lbl_* on the gminor_attributed_training row.
 *
 * MUST stay byte-identical to codegen/gminor_schema_catalog.py :: LABEL_COLS. Scala can't read the
 * Python list at build time, so both are hand-maintained; the codegen structural lint asserts the
 * output-contract lbl_* set derives from this list, but the ordering / membership here is the
 * authoritative Scala mirror. If you edit one, edit both.
 */
object GminorConfig {
  val LABEL_COLS: Seq[String] = Seq(
    "jgr_settlement_price", "jgr_settlement_status", "jgr_winning_bid_price",
    "jgr_min_bid_to_win", "jgr_second_place_price", "jgr_no_serv_reason",
    "jgr_winner_predicted_nr", "jgr_vungle_price", "jgr_is_winner_acc"
  )
}

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

  // Output-contract metric columns for one aggregate side: read the dl_/ndc_ names from the json
  // contract, drop the two non-feature columns (agg_hit / agg_event_time are computed here), and
  // map each back to the un-prefixed aggregate column (a.<metric> AS <prefixed>). Kept in sync with
  // columns/gminor_attributed_training.json by construction — the prefix strip is exact.
  private def prefixedMetricSelect(prefix: String): Seq[String] =
    getColsInJson("columns/gminor_attributed_training.json")
      .filter(_.startsWith(prefix))
      .filterNot(Set(s"${prefix}agg_hit", s"${prefix}agg_event_time"))
      .map(c => s"a.${c.substring(prefix.length)} AS $c")

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

    // 2) wide bridge -> both surrogate keys (KeySpec, same as agg) + labels, event grain.
    // Inner projects the raw dimension columns; outer computes the sha2 surrogate over them so the
    // device_dim_id / context_dim_id are byte-identical to realtime_attributed_aggregation.
    //
    // Wide table grain is (event_id, imp_id) -- an event can carry up to several served/attempted
    // impressions -- but GMinor is event-grain, so this bridge MUST emit exactly one row per
    // event_id. We collapse via row_number() choosing a deterministic representative impression:
    // prefer the delivered/served impression (jgr_no_serv_reason = 0), tie-break on lowest imp_id.
    // Without this, the LEFT JOIN below fans out per event_id and the later PIT row_number() picks
    // an arbitrary fanned row, making context_dim_id / lbl_* nondeterministic per event.
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
            FROM (
              SELECT event_id, imp_id,
                     $dimProj,
                     ${LABEL_COLS.mkString(", ")},
                     row_number() OVER (
                       PARTITION BY event_id
                       ORDER BY (CASE WHEN jgr_no_serv_reason = 0 THEN 0 ELSE 1 END) ASC, imp_id ASC
                     ) AS _imp_rn
                FROM $wideTable
               WHERE source_event_time >= '$s' AND source_event_time < '$t'
            )
           WHERE _imp_rn = 1
        )
    """).createOrReplaceTempView("_wide_keyed")

    // Attach keys + labels to gminor. LEFT JOIN keeps unmatched gminor rows (wide_join_hit=false,
    // null keys/labels -> both agg stages will also miss and null-fill).
    val labelJoinSel = LABEL_COLS.map(c => s"w.lbl_$c").mkString(", ")
    spark.sql(s"""
      SELECT g.*, w.device_dim_id, w.context_dim_id, (w.event_id IS NOT NULL) AS wide_join_hit,
             $labelJoinSel
        FROM _gminor g LEFT JOIN _wide_keyed w ON g.event_id = w.event_id
    """).createOrReplaceTempView("_keyed")

    // 3) point-in-time device + context joins.
    // For each aggregate side: LEFT JOIN the agg on the surrogate key, restricting to snapshots in
    // the STRICT prior hour window [event_hour - lookback, event_hour) — agg.event_time < event_hour
    // (NOT < source_event_time; same-hour aggregates would leak). row_number() DESC per event_id
    // keeps the single latest surviving snapshot; = 1 also keeps the lone null row on a miss.
    def pitStage(inView: String, aggTable: String, keyCol: String, prefix: String, outView: String): Unit = {
      val metricSel = prefixedMetricSelect(prefix).mkString(",\n             ")
      spark.sql(s"""
        WITH _joined AS (
          SELECT k.*,
                 a.$keyCol AS ${prefix}_matched_key,
                 a.event_time AS ${prefix}agg_event_time,
                 $metricSel,
                 row_number() OVER (
                   PARTITION BY k.event_id ORDER BY a.event_time DESC
                 ) AS _rn
            FROM $inView k
            LEFT JOIN $aggTable a
              ON a.$keyCol = k.$keyCol
             AND a.event_time <  k.event_hour
             AND a.event_time >= k.event_hour - INTERVAL '$lookbackH' HOUR
        )
        SELECT * EXCEPT (_rn, ${prefix}_matched_key),
               (${prefix}_matched_key IS NOT NULL) AS ${prefix}agg_hit
          FROM _joined
         WHERE _rn = 1
      """).createOrReplaceTempView(outView)
    }

    // Chain device -> context: context stage reads the device stage's output.
    pitStage("_keyed",       deviceAgg,  "device_dim_id",  "dl_",  "_keyed_dl")
    pitStage("_keyed_dl",    contextAgg, "context_dim_id", "ndc_", "_keyed_dl_ndc")

    // appendToIcebergTable aligns by name against the full-contract DDL and null-fills any output
    // columns not selected here (e.g. columns present in the DDL but not emitted by an agg side).
    val out = spark.table("_keyed_dl_ndc")
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
