package com.vungle.signalprism.data.hb_transactions_ingestion

import com.vungle.lena.{BoilerplateSparkMain, UDFUtil}
import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC
import org.joda.time.format.DateTimeFormat

/**
 * Consume coba2.hb_transactions (the full-payload coba2 landing of the hb-transactions topic,
 * produced upstream by coba/ingestion2). Event-id sample, keep the served/winning bid per
 * (event_id, bidrequest_imp_id) via row_number, project all hb-sourced wide-table columns,
 * and write ml_shadow.hb_transactions_wide_staging. Modeled on hbp/auctions_served_ingestion.
 */
object SparkMain extends BoilerplateSparkMain {
  private val NS = "spark.app.signalprism.data.hb_transactions_ingestion"

  def requiredArgs: List[String] = List(
    "spark.app.env",
    "spark.app.batch_jobs.db.url",
    s"$NS.input.s3Dir",
    s"$NS.input.topic",
    s"$NS.output.tableName"
  )

  def defaultArgs: Map[String, String] = Map(
    "spark.rdd.compress" -> "true",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    s"$NS.test.create.table" -> "false",
    s"$NS.output.s3Dir" -> "",
    s"$NS.merge.lookback.days" -> "7",
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

  lazy val projection = getColsMapInJson("col_maps/hb_transactions_wide.json")
    .map { case (k, v) => s"$k AS $v" }.mkString(",\n  ")

  // scalastyle:off
  def process(next: DateTime, tempTable: String): Unit = {
    val startMillis = System.currentTimeMillis()
    // NB: hbn_timestamp comes from the col_map ($projection maps `timestamp` -> `hbn_timestamp`),
    // so it must NOT be emitted again here, or the output would have a duplicate column.
    val merged = spark.sql(
      s"""
        SELECT event_id, bidrequest_imp_id AS imp_id, $projection
        FROM (
          SELECT *,
                 row_number() OVER (
                   PARTITION BY event_id, bidrequest_imp_id ORDER BY timestamp
                 ) AS _rn
          FROM $tempTable
          WHERE in_user_sample(sha1(event_id), $SAMPLE_RATE)
        )
        WHERE _rn = 1
      """)
    mergeToIcebergTable(outputTable, merged, lookbackDays, mergeKeysAllowNull = Array("imp_id"))
    reportStatsMetric(s"$appName.write.seconds", (System.currentTimeMillis() - startMillis) / 1000)
  }
  // scalastyle:on

  def run: Unit = {
    UDFUtil.registerCommonUDF(spark)
    UDFUtil.registerInUserSample(spark)

    assertTestTableName(outputTable)
    if (isTest && testCreateTable) {
      assertTestS3(outputS3Base)
      createTestTable(outputTable, outputS3Base, "sql/hb_transactions_wide_staging.template")
    }

    val pattern = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
    val tillTimeStr = getTillTime(s"$NS.till", "yyyy-MM-dd HH:mm")
    val tillTime = DateTime.parse(tillTimeStr, pattern).toDateTime(UTC)

    withCoba2TempViewInRange(S3base, topic, tillTime.minusHours(1), tillTime, "_hb_served") {
      case (tempTable, _) => process(tillTime, tempTable)
    }
    if (isNotBackfill) recordProgressMinute(tillTime)
    reportStatsMetric(s"$appName.success", 1)
  }
}
