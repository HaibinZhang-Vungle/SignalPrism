package com.vungle.signalprism.data.jaeger_transaction_ingestion

import com.vungle.lena.{BoilerplateSparkMain, UDFUtil}
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * Consume coba2.ex_jaeger_transaction (the full-payload coba2 landing of the
 * ex-jaeger-transaction topic, produced upstream by coba/ingestion2). Explode
 * placement_serve_results[] -> placements[] -> winning rtbconnections[], event-id sample,
 * project all jaeger-sourced wide-table columns, and write
 * ml_shadow.jaeger_transaction_wide_staging at (event_id, imp_id) grain.
 *
 * Projection is driven by col_maps/jaeger_transaction_wide.json (source-expr -> target),
 * generated from schemas/realtime_attributed_wide_table_schema.md.
 */
object SparkMain extends BoilerplateSparkMain {
  private val NS = "spark.app.signalprism.data.jaeger_transaction_ingestion"

  def requiredArgs: List[String] = List(
    "spark.app.env",
    "spark.app.batch_jobs.db.url",
    s"$NS.input.s3Dir",       // coba2 S3 base, e.g. s3a://.../coba2
    s"$NS.input.topic",       // "ex-jaeger-transaction"
    s"$NS.output.tableName"   // ml_shadow.jaeger_transaction_wide_staging
  )

  def defaultArgs: Map[String, String] = Map(
    "spark.rdd.compress" -> "true",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    s"$NS.test.create.table" -> "false",
    s"$NS.output.s3Dir" -> "",
    s"$NS.merge.lookback.days" -> "1",
    s"$NS.sample_rate" -> "0.0001",
    "spark.speculation" -> "false",
    "spark.hadoop.fs.s3a.fast.upload" -> "true",
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version" -> "2"
  )

  lazy val testCreateTable = args(s"$NS.test.create.table").toBoolean
  lazy val outputTable     = args(s"$NS.output.tableName")
  lazy val S3base          = args(s"$NS.input.s3Dir")
  lazy val outputS3Base    = args(s"$NS.output.s3Dir")
  lazy val topic           = args(s"$NS.input.topic")
  lazy val lookbackDays    = args(s"$NS.merge.lookback.days").toInt
  lazy val SAMPLE_RATE     = args(s"$NS.sample_rate").toDouble

  def registerSparkUDF(spark: SparkSession): Unit = {
    UDFUtil.registerNormalizeDeviceId(spark)
    UDFUtil.registerFormatCountry(spark)
    UDFUtil.registerFormatId(spark)
    UDFUtil.registerExtractAdomain(spark)
    UDFUtil.registerMappingTemplateName(spark)
    UDFUtil.registerInUserSample(spark)
    UDFUtil.registerMongoIdToTimestamp(spark)
  }

  // Collect every resolvable dotted path in a schema (descends into structs and array<struct>
  // elements), so we can project only the col_map sources that actually exist in the input.
  private def resolvablePaths(dt: org.apache.spark.sql.types.DataType, prefix: String): Set[String] = {
    import org.apache.spark.sql.types.{StructType, ArrayType}
    dt match {
      case st: StructType => st.fields.flatMap { f =>
          val p = if (prefix.isEmpty) f.name else prefix + "." + f.name
          resolvablePaths(f.dataType, p) + p
        }.toSet
      case ArrayType(el, _) => resolvablePaths(el, prefix)
      case _ => Set.empty[String]
    }
  }

  // scalastyle:off
  def process(next: DateTime, tempTable: String): Unit = {
    val startMillis = System.currentTimeMillis()

    // Explode to (event_id, imp_id) grain with the winning RTB connection, then materialize so we
    // can inspect its schema before projecting.
    val cteSql =
      s"""
        WITH served AS (
            SELECT *,
                   explode(placement_serve_results) AS serve_result
              FROM $tempTable
        ),
        served_sampled AS (
            SELECT *
              FROM served
             WHERE in_user_sample(sha1(serve_result.ad_event_id), $SAMPLE_RATE)
               AND serve_result.ad_event_id IS NOT NULL
        ),
        served_placement AS (
            SELECT *,
                   explode(placements) AS placement_
              FROM served_sampled
        ),
        served_rtb AS (
            SELECT *,
                   explode(serve_result.rtbconnections) AS rtb_conn
              FROM served_placement
             WHERE serve_result.winner_id IS NOT NULL
               AND serve_result.placement_reference_id = placement_.reference_id
        )
        SELECT *,
               format_id(serve_result.ad_event_id) AS event_id,
               serve_result.imp_id                 AS imp_id,
               timestamp                           AS source_event_time
          FROM served_rtb
         WHERE serve_result.winner_id = rtb_conn.id
      """
    logExplain(cteSql, s"jaeger ingestion from coba2 $topic")
    spark.sql(cteSql).createOrReplaceTempView("_jaeger_winner")

    // The schema md lists some jaeger fields not present in the actual parquet, so project ONLY
    // the col_map sources that resolve against the exploded schema; append null-fills the rest.
    // jgr_winner_account_id is emitted directly (shares source expr rtb_conn.account_id with
    // jgr_rtb_account_id, so it is excluded from the col_map).
    val paths = resolvablePaths(spark.table("_jaeger_winner").schema, "")
    val colTransSpec = getColsMapInJson("col_maps/jaeger_transaction_wide.json").toSeq
      .filter { case (src, _) => paths.contains(src) }
      .map { case (src, tgt) => s"$src AS $tgt" }
      .mkString(",\n  ")

    val out = spark.sql(
      s"""
        SELECT event_id,
               imp_id,
               rtb_conn.account_id AS jgr_winner_account_id,
               $colTransSpec
          FROM _jaeger_winner
         WHERE event_id IS NOT NULL
      """).dropDuplicates("event_id", "imp_id")

    appendToIcebergTable(outputTable, out)
    reportStatsMetric(s"$appName.write.seconds", (System.currentTimeMillis() - startMillis) / 1000)
  }
  // scalastyle:on

  def run: Unit = {
    assertTestTableName(outputTable)
    if (isTest && testCreateTable) {
      assertTestS3(outputS3Base)
      createTestTable(outputTable, outputS3Base, "sql/jaeger_transaction_wide_staging.template")
    }
    registerSparkUDF(spark)

    val tillTimeStr = getTillTime(s"$NS.till", "yyyy-MM-dd HH:mm")
    val tillTime = DateTime.parse(tillTimeStr, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm"))
    val tillDay = tillTime.toString("yyyy-MM-dd")
    val tillHour = tillTime.toString("HH")
    val tillMinute = tillTime.toString("mm")

    var nextStartAdjusted: DateTime = null
    for ((nextStart, nextTill) <- hourlyPeriodsForCurrentBatchMinute(tillDay, tillHour, tillMinute)) {
      assert(isTimeBeforeNow(nextTill))
      if (nextStartAdjusted == null) nextStartAdjusted = nextStart

      var hasDataProcessed = false
      withCoba2TempViewInRange(S3base, topic, nextStartAdjusted, nextTill, "_jaeger", storeS3IngestTime = true) {
        case (tempTable, upperTimeBound) =>
          process(nextStart, tempTable)
          nextStartAdjusted = upperTimeBound.plusMinutes(1)
          hasDataProcessed = true
      }
      if (isNotBackfill && hasDataProcessed) recordProgressMinute(nextStartAdjusted)
      reportStatsMetric(s"$appName.success", 1)
    }
  }
}
