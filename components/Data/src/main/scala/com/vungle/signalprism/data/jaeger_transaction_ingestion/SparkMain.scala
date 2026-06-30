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

  // scalastyle:off
  def process(next: DateTime, tempTable: String): Unit = {
    val startMillis = System.currentTimeMillis()

    // source-expr -> target, e.g. "serve_result.bid_floor" -> "jgr_bid_floor",
    // "rtb_conn.account_id" -> "jgr_winner_account_id", "device.make" -> "jgr_dev_make".
    val columnMap = getColsMapInJson("col_maps/jaeger_transaction_wide.json")
    val colTransSpec = columnMap.map { case (k, v) => s"$k AS $v" }.mkString(",\n")

    val transformSql =
      s"""
        WITH served AS (
            SELECT *,
                   explode(placement_serve_results) AS serve_result
              FROM $tempTable
             WHERE in_user_sample(sha1(placement_serve_results.ad_event_id[0]), $SAMPLE_RATE)
                OR in_user_sample(sha1(id), $SAMPLE_RATE)
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
             WHERE serve_result.placement_reference_id = placement_.reference_id
        ),
        served_rtb AS (
            SELECT *,
                   explode(serve_result.rtbconnections) AS rtb_conn
              FROM served_placement
             WHERE serve_result.winner_id IS NOT NULL
        ),
        served_winner AS (
            SELECT *,
                   format_id(serve_result.ad_event_id) AS event_id,
                   serve_result.imp_id                 AS imp_id,
                   timestamp                           AS source_event_time
              FROM served_rtb
             WHERE serve_result.winner_id = rtb_conn.id
        )
        SELECT event_id,
               imp_id,
               rtb_conn.account_id AS jgr_winner_account_id,
               $colTransSpec,
               CAST('${toMinutelyTimeStr(next)}' AS timestamp) AS ingest_time
          FROM served_winner
         WHERE event_id IS NOT NULL
      """
    logExplain(transformSql, s"jaeger ingestion from coba2 $topic")
    val out = spark.sql(transformSql).dropDuplicates("event_id", "imp_id")

    mergeToIcebergTable(outputTable, out, lookbackDays, mergeKeysAllowNull = Array("imp_id"))
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
