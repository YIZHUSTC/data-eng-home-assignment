import java.time.LocalDate

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

case class TimeAggQuery(fromDate: LocalDate,
                        toDate: LocalDate,
                        stepDays: Int,
                        filter: Option[Column],
                        distinct: Option[Column],
                        by: Option[Column])

class TimeAggQueryExecutor(spark: SparkSession) {

  import spark.implicits._

  def execute(input: DataFrame, query: TimeAggQuery): DataFrame = {

    import java.time.format.DateTimeFormatter
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.functions.row_number
    import org.apache.spark.sql.expressions.Window
    import java.time.{LocalDate, LocalDateTime, ZoneId}

    lazy val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    implicit def stringToTs(str: String): Long = LocalDateTime.parse(str, dateTimeFormatter).atZone(ZoneId.of("America/Los_Angeles")).toEpochSecond
    lazy val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    implicit def stringToLocalDate(str: String): LocalDate = LocalDate.parse(str, dateFormatter)

    val fromTs = stringToTs(query.fromDate + " 00:00:00")
    val toTs = stringToTs(query.toDate + " 23:59:59")
    var df = input.toDF().filter($"ts" >= fromTs && $"ts" <= toTs).
      withColumn("slot", (($"ts" - fromTs) / query.stepDays / 86400).cast("integer"))

    if (query.filter.getOrElse("notspecified") != "notspecified") {
      df = df.filter(query.filter.get)
    }

    if (query.distinct.getOrElse("notspecified") != "notspecified") {
      val w = Window.partitionBy($"slot", col(query.distinct.get.toString())).orderBy($"ts")
      df = df.withColumn("rank", row_number().over(w)).where($"rank" === 1)
    }

    if (query.by.getOrElse("notspecified") != "notspecified") {
      df = df.groupBy("slot", query.by.get.toString()).count().
        withColumnRenamed(query.by.get.toString(), "by").sort($"slot", $"by")
    }
    else {
      df = df.groupBy("slot").count().sort("slot")
    }

    return df

  }

}
