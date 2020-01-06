package com.example

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions.Window


case class Crime(
                  INCIDENT_NUMBER: Option[String],
                  OFFENSE_CODE: Option[Int],
                  OFFENSE_CODE_GROUP: Option[String],
                  OFFENSE_DESCRIPTION: Option[String],
                  DISTRICT: Option[String],
                  REPORTING_AREA: Option[String],
                  SHOOTING: Option[String],
                  OCCURRED_ON_DATE: Option[Timestamp],
                  YEAR: Option[Int],
                  MONTH: Option[Int],
                  DAY_OF_WEEK: Option[String],
                  HOUR: Option[Int],
                  UCR_PART: Option[String],
                  STREET: Option[String],
                  Lat: Option[Double],
                  Long: Option[Double],
                  Location: Option[String]
                )

case class OffenceCodes(
                         CODE: Option[Int],
                         NAME: Option[String]
                       )

object BostonCrimesMap extends App {
  val spark = SparkSession.builder().master("local").getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  //val filename = args(0) //"winemag-data-130k-v2.json" //args(0)
  //val decodeUser = sc.textFile(filename)
  //decodeUser.foreach(i => println(parse(i).extract[Users]))
  val dfCrime = spark.read.format("csv")
    .option("delimiter ", ",").option("header", "true").option("inferSchema", true).load("source/crime.csv").as[Crime]

  val dfCrime1 = dfCrime
    .select(dfCrime("DISTRICT").as("district"),
      date_trunc("Day", dfCrime("OCCURRED_ON_DATE")).as("OCCURRED_ON_DATE"))
    .where($"district" === "A1")
    .groupBy($"district",
      $"OCCURRED_ON_DATE")
    .agg(count("*").as("crimes_total")
    )
    .select($"district",
      $"crimes_total",
      $"OCCURRED_ON_DATE"
      //      avg(dfCrime("Lat")),
      //      avg(dfCrime("Long"))
    )
    .orderBy($"OCCURRED_ON_DATE")


  val dfCrime12 = dfCrime
    .select(dfCrime("DISTRICT").as("district"),
      $"OFFENSE_CODE")
    .where($"district" === "A1")
    .groupBy($"district",
      $"OFFENSE_CODE")
    .agg(count("OFFENSE_CODE").as("cnt_offense_code")
    )
    .withColumn("rn", row_number().over(Window.orderBy($"cnt_offense_code".desc)))
    .select($"district",
      $"OFFENSE_CODE",
      $"rn"
    )
    .orderBy($"rn")
  println(dfCrime12.show(300))

  val dfCrime11 = dfCrime1.withColumn("date_month", trunc($"OCCURRED_ON_DATE", "mm"))
    .groupBy($"district", $"date_month")
    .agg(callUDF("percentile_approx", $"crimes_total", lit(0.5)).as("mediana"),
      sum($"crimes_total").as("sum_crimes_total"))
    .withColumn("sumCrimesTotal", sum($"sum_crimes_total").over(Window.partitionBy($"district")))
    .orderBy($"date_month")

  // println(dfCrime11.show(1000))
  //dfCrime1.createTempView("dfCrime1")
  //  val dfCrime2 = spark.sql(
  //    """Select district,trunc(OCCURRED_ON_DATE,"mm") as date,
  //      | percentile_approx(crimes_total,0.5) as mediana,
  //      | sum(crimes_total) as sum_crimes_total
  //      | from dfCrime1
  //      | group by district, trunc(OCCURRED_ON_DATE,"mm")
  //      | order by date""".stripMargin)
  //  println(dfCrime2.show())
  //  val dfOffenceCodes = spark.read.format("csv")
  //    .option("delimiter ", ",")
  //    .option("header", "true")
  //    .option("inferSchema", true)
  //    .load("source/offense_codes.csv").as[OffenceCodes]


  //  val df = dfCrime
  //    .join(broadcast(dfOffenceCodes), dfCrime("OFFENSE_CODE") === dfOffenceCodes("CODE"))
  //    .select(dfCrime("DISTRICT").as("district"))
  //    .groupBy($"district")
  //    .agg(count("*").as("crimes_total"))
  //    .select($"district",
  //      $"crimes_total"
  //      //      avg(dfCrime("Lat")),
  //      //      avg(dfCrime("Long"))
  //    )
  //    .orderBy($"district")
  //  println(df.show())
}