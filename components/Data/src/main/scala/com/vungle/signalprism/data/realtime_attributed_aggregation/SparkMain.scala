package com.vungle.signalprism.data.realtime_attributed_aggregation

import com.vungle.lena.{BoilerplateSparkMain, UDFUtil}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scala.util.parsing.json.JSON

/**
 * Hourly aggregation over ml_shadow.realtime_attributed_event_wide into one of two reviewed
 * dimension families (device_level_v1 / non_device_context_v1), selected by dimension_family.
 *
 * Contract: schemas/realtime_attributed_aggregation_table_schema.md. The dimension list, per-dim
 * normalization, surrogate-key recipe and metric catalog are read at runtime from the codegen'd
 * resources (agg_specs/<family>.json, agg_specs/metric_catalog.json) so no metric-specific code
 * path exists. Only 'computed' metrics are emitted; predicate-dependent / absent-source metrics
 * are left out and appendToIcebergTable null-fills them against the full-contract DDL.
 */
object SparkMain extends BoilerplateSparkMain {
  private val NS = "spark.app.signalprism.data.realtime_attributed_aggregation"

  def requiredArgs: List[String] = List(
    "spark.app.env",
    "spark.app.batch_jobs.db.url",
    s"$NS.dimension_family",
    s"$NS.input.tableName",
    s"$NS.output.tableName"
  )

  def defaultArgs: Map[String, String] = Map(
    "spark.rdd.compress" -> "true",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    s"$NS.test.create.table" -> "false",
    s"$NS.output.s3Dir" -> "",
    s"$NS.sample_rate" -> "1.0",
    s"$NS.aggregation_version" -> "v1_computed_only",
    "spark.speculation" -> "false",
    "spark.hadoop.fs.s3a.fast.upload" -> "true",
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version" -> "2"
  )

  lazy val testCreateTable = args(s"$NS.test.create.table").toBoolean
  lazy val family      = args(s"$NS.dimension_family")
  lazy val inputTable  = args(s"$NS.input.tableName")
  lazy val outputTable = args(s"$NS.output.tableName")
  lazy val outputS3    = args(s"$NS.output.s3Dir")
  lazy val sampleRate  = args(s"$NS.sample_rate").toDouble
  lazy val aggVersion  = args(s"$NS.aggregation_version")

  // ---- spec loading (scala.util.parsing.json keeps this dependency-free) ----
  private def json(res: String): Map[String, Any] =
    JSON.parseFull(getResourceString(res)).get.asInstanceOf[Map[String, Any]]
  private def arr(a: Any): List[Any] = a.asInstanceOf[List[Any]]
  private def obj(a: Any): Map[String, Any] = a.asInstanceOf[Map[String, Any]]
  private def str(a: Any): String = if (a == null) null else a.toString

  lazy val spec        = json(s"agg_specs/$family.json")
  lazy val metricCat   = json("agg_specs/metric_catalog.json")
  lazy val primaryKey  = str(obj(spec("primary_key"))("name"))
  lazy val dropNullSrc = str(spec.getOrElse("drop_null_source", null))
  lazy val dims        = arr(spec("dimensions")).map(obj)

  // normalized, still-nullable stored dimension expression
  private def dimExpr(d: Map[String, Any]): String = {
    val name = str(d("name")); val src = str(d("source_col")); val norm = str(d("norm"))
    val e = norm match {
      case _ if src == null       => "CAST(NULL AS STRING)"
      case "parse_major"          => s"split($src, '\\\\.')[0]"
      case "coalesce"             => s"coalesce($src, ${str(d("fallback_col"))})"
      case "normalize" | "bucket" => s"lower(trim($src))"  // bucket: top-N deferred → normalized passthrough
      case "expr"                 => src
      case _                      => src
    }
    s"$e AS $name"
  }

  // key concat coalesces every dim to '__unknown__' (contract §2)
  private def keyConcatArg(d: Map[String, Any]): String =
    s"coalesce(CAST(${str(d("name"))} AS STRING), '__unknown__')"

  private def computedMetricSelects: Seq[String] = {
    val dist = arr(metricCat("distribution")).map(obj)
      .filter(m => str(m("kind")) == "computed")
      .flatMap { m =>
        val fam = str(m("family")); val e = str(m("base_expr"))
        Seq(
          s"sum($e) AS ${fam}_sum",
          s"count($e) AS ${fam}_count",
          s"min($e) AS ${fam}_min",
          s"max($e) AS ${fam}_max",
          s"sum($e * $e) AS ${fam}_squaresum"
        )
      }
    val counts = arr(metricCat("count")).map(obj)
      .filter(m => str(m("kind")) == "computed")
      .map { m =>
        val n = str(m("name")); val p = str(m("predicate"))
        s"sum(CASE WHEN $p THEN 1 ELSE 0 END) AS $n"
      }
    dist ++ counts
  }

  // scalastyle:off
  def process(start: DateTime, till: DateTime): Unit = {
    val startMillis = System.currentTimeMillis()
    val s = start.toString("yyyy-MM-dd HH:mm:ss")
    val t = till.toString("yyyy-MM-dd HH:mm:ss")

    val dimStored  = dims.map(dimExpr).mkString(",\n    ")
    val dimNames   = dims.map(d => str(d("name")))
    val keyArgs    = dims.map(keyConcatArg).mkString(", ")
    val dropClause = if (dropNullSrc != null) s"AND $dropNullSrc IS NOT NULL" else ""

    // Single scan: normalized dims + hour bucket alongside the raw metric base columns (`*`),
    // so dims and metric sources coexist for the outer GROUP BY.
    spark.sql(
      s"""
        SELECT
          $dimStored,
          date_trunc('HOUR', source_event_time) AS event_time,
          *
        FROM $inputTable
        WHERE source_event_time >= '$s' AND source_event_time < '$t'
          AND in_user_sample(sha1(event_id), $sampleRate)
          $dropClause
      """).createOrReplaceTempView("_agg_src")

    val groupCols = (dimNames :+ "event_time").mkString(", ")
    val metricSel = computedMetricSelects.mkString(",\n    ")

    val outSql =
      s"""
        SELECT
          event_time,
          date_format(event_time, 'yyyy-MM-dd-HH') AS ingest_time,
          substr(sha2(concat_ws('|', $keyArgs), 256), 1, 2) AS hashid,
          sha2(concat_ws('|', $keyArgs), 256) AS $primaryKey,
          ${dimNames.mkString(",\n          ")},
          $metricSel,
          count(*) AS source_event_count,
          min(source_event_time) AS first_source_event_time,
          max(source_event_time) AS last_source_event_time,
          '$aggVersion' AS aggregation_version
        FROM _agg_src
        GROUP BY $groupCols
      """
    logExplain(outSql, s"aggregate $inputTable -> $outputTable [$family]")
    val agg = spark.sql(outSql)
    appendToIcebergTable(outputTable, agg)
    reportStatsMetric(s"$appName.write.seconds", (System.currentTimeMillis() - startMillis) / 1000)
  }
  // scalastyle:on

  def run: Unit = {
    UDFUtil.registerCommonUDF(spark)
    UDFUtil.registerInUserSample(spark)

    assertTestTableName(outputTable)
    if (isTest && testCreateTable) {
      assertTestS3(outputS3)
      val tmpl =
        if (family == "device_level_v1") "sql/realtime_attributed_device_level_hly.template"
        else "sql/realtime_attributed_non_device_context_hly.template"
      createTestTable(outputTable, outputS3, tmpl)
    }

    val pattern = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val finalTill = if (isNotBackfill) {
      val progress = checkoutProgressTimeCompatible.get
      val src = checkoutProgress("signalprism.data.realtime_attributed_wide", "default")
        .map(x => parseTimeCompatible(x).get)
        .getOrElse(throw new Exception("realtime_attributed_wide progress not found!"))
      if (!progress.isBefore(src)) { _logger.warn(s"ProgressTime [$progress] >= till [$src]. Skipping."); return }
      src
    } else {
      DateTime.parse(getTillTime(s"$NS.till", "yyyy-MM-dd HH:mm:ss"), pattern)
    }

    val tillDay = finalTill.toString("yyyy-MM-dd")
    val tillHour = finalTill.toString("HH")
    val tillMinute = finalTill.toString("mm")
    for ((nextStart, nextTill) <- hourlyPeriodsForCurrentBatchMinute(tillDay, tillHour, tillMinute)) {
      assert(isTimeBeforeNow(nextTill))
      process(nextStart, nextTill)
      if (isNotBackfill) recordProgressMinute(nextTill)
    }
    reportStatsMetric(s"$appName.success", 1)
  }
}
