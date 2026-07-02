package com.vungle.signalprism.data.realtime_attributed_wide

import com.vungle.lena.{BoilerplateSparkMain, UDFUtil}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * Join ml_shadow.jaeger_transaction_wide_staging (LEFT) to ml_shadow.hb_transactions_wide_staging
 * on (event_id, imp_id) and write ml_shadow.realtime_attributed_event_wide at (event_id, imp_id)
 * grain. Content-only: emits exactly the schema-md columns. HB §3.1 dup copies are already absent
 * from the hb staging table, so jaeger columns win automatically.
 */
object SparkMain extends BoilerplateSparkMain {
  private val NS = "spark.app.signalprism.data.realtime_attributed_wide"

  def requiredArgs: List[String] = List(
    "spark.app.env",
    "spark.app.batch_jobs.db.url",
    s"$NS.input.jaegerTable",
    s"$NS.input.hbTable",
    s"$NS.output.tableName"
  )

  def defaultArgs: Map[String, String] = Map(
    "spark.rdd.compress" -> "true",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    s"$NS.test.create.table" -> "false",
    s"$NS.output.s3Dir" -> "",
    s"$NS.merge.lookback.days" -> "1",
    s"$NS.lookback_valid_hours" -> "4",
    "spark.speculation" -> "false",
    "spark.hadoop.fs.s3a.fast.upload" -> "true",
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version" -> "2"
  )

  lazy val testCreateTable = args(s"$NS.test.create.table").toBoolean
  lazy val jaegerTable     = args(s"$NS.input.jaegerTable")
  lazy val hbTable         = args(s"$NS.input.hbTable")
  lazy val outputTable     = args(s"$NS.output.tableName")
  lazy val outputS3Base    = args(s"$NS.output.s3Dir")
  lazy val lookbackDays    = args(s"$NS.merge.lookback.days").toInt
  lazy val lookbackHours   = args(s"$NS.lookback_valid_hours").toInt

  // Columns sourced from the jaeger side vs the hb side (excluding shared keys).
  lazy val jaegerCols = getColsInJson("columns/jaeger_transaction_wide.json")
    .diff(List("event_id", "imp_id"))
  lazy val hbCols = getColsInJson("columns/hb_transactions_wide.json")
    .diff(List("event_id", "imp_id"))

  lazy val jaegerSelect = jaegerCols.map(c => s"j.$c AS $c").mkString(",\n  ")
  lazy val hbSelect     = hbCols.map(c => s"h.$c AS $c").mkString(",\n  ")

  // scalastyle:off
  def process(start: DateTime, till: DateTime): Unit = {
    val startMillis = System.currentTimeMillis()
    val s = start.toString("yyyy-MM-dd HH:mm:ss")
    val t = till.toString("yyyy-MM-dd HH:mm:ss")

    withEnableStoragePartitionJoin() { () =>
      val sql =
        s"""
          SELECT
            j.event_id AS event_id,
            j.imp_id   AS imp_id,
            $jaegerSelect,
            $hbSelect
          FROM $jaegerTable j
          LEFT JOIN $hbTable h
            ON j.event_id = h.event_id
          WHERE j.source_event_time >= '$s' AND j.source_event_time < '$t'
        """
      logExplain(sql, s"join $jaegerTable with $hbTable")
      val wide = spark.sql(sql).dropDuplicates("event_id", "imp_id")

      // Go/no-go metric: attribution hit rate (schema §2.3).
      wide.createOrReplaceTempView("_wide")
      val hr = spark.sql(
        """SELECT
             SUM(CASE WHEN hbn_bidrequest_id IS NOT NULL THEN 1 ELSE 0 END) AS hit,
             COUNT(*) AS total
           FROM _wide""").collect()(0)
      val total = hr.getAs[Long]("total")
      if (total > 0) {
        reportStatsMetric(s"$appName.attribution_hit_rate", (hr.getAs[Long]("hit") * 1000 / total))
      }

      appendToIcebergTable(outputTable, wide)
      reportStatsMetric(s"$appName.write.seconds", (System.currentTimeMillis() - startMillis) / 1000)
    }
  }
  // scalastyle:on

  def run: Unit = {
    UDFUtil.registerCommonUDF(spark)

    assertTestTableName(outputTable)
    if (isTest && testCreateTable) {
      assertTestS3(outputS3Base)
      createTestTable(outputTable, outputS3Base, "sql/realtime_attributed_event_wide.template")
    }

    val pattern = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val finalTill = if (isNotBackfill) {
      val progressTime = checkoutProgressTimeCompatible.get
      val jaegerProgress = checkoutProgress("signalprism.data.jaeger_transaction_ingestion", "default")
        .map(x => parseTimeCompatible(x).get)
        .getOrElse(throw new Exception("jaeger_transaction_ingestion progress not found!"))
      val hbProgress = checkoutProgress("signalprism.data.hb_transactions_ingestion", "default")
        .map(x => parseTimeCompatible(x).get)
        .getOrElse(throw new Exception("hb_transactions_ingestion progress not found!"))
      val safeTill = hbProgress.minusHours(lookbackHours)
      val till = if (jaegerProgress.isBefore(safeTill)) jaegerProgress else safeTill
      if (!progressTime.isBefore(till)) {
        _logger.warn(s"ProgressTime [$progressTime] >= till [$till]. Skipping.")
        return
      }
      till
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
