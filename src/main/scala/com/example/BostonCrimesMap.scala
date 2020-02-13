package com.example

import java.sql.Timestamp

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}
import org.apache.log4j.{Level, Logger}

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

case class bostoncrimes(district: Option[String],
                        crimes_total: Option[BigInt],
                        crimes_monthly: Option[BigInt],
                        frequent_crime_types: Option[String],
                        lat: Option[Double],
                        lng: Option[Double]
                       )

object BostonCrimesMap extends App {
  val spark = SparkSession.builder().master("local").getOrCreate()

  import spark.implicits._

  Logger.getLogger("org").setLevel(Level.OFF)

  val sc = spark.sparkContext
  val dfCrime = spark.read.format("csv")
    .option("delimiter ", ",").option("header", "true").option("inferSchema", true).load(args(0)).as[Crime]

  val dfOffenceCodes = spark.read.format("csv")
    .option("delimiter ", ",").option("header", "true").option("inferSchema", true).load(args(1)).as[OffenceCodes]
    .distinct()
    .dropDuplicates("CODE")
  println(dfOffenceCodes.count())
  println(dfOffenceCodes.distinct().count())


  val crimeType = dfCrime
    .select(coalesce(dfCrime("DISTRICT"), lit("null")).as("district"),
      $"OFFENSE_CODE",
      $"Lat",
      $"Long")
    .withColumn("lat", avg($"Lat").over(Window.partitionBy($"district")))
    .withColumn("lng", avg($"Long").over(Window.partitionBy($"district")))
    .groupBy($"district", $"lat", $"lng",
      $"OFFENSE_CODE".as("offense_code"))
    .agg(count("OFFENSE_CODE").as("cnt_offense_code"))
    .withColumn("rnk", dense_rank().over(Window.partitionBy($"district").orderBy($"cnt_offense_code".desc)))
    .where($"rnk" < 4)
    .orderBy($"district", $"rnk")
    .join(broadcast(dfOffenceCodes), $"offense_code" === dfOffenceCodes("CODE"))
    .select($"district",
      $"offense_code",
      $"cnt_offense_code",
      $"rnk",
      $"NAME",
      $"lat",
      $"lng")
    .withColumn("crime_type", lit(trim(split($"NAME", "-").getItem(0))))
    .orderBy($"district", $"rnk")
    .groupBy($"district", $"lat", $"lng")
    .agg(concat_ws(", ", collect_list("crime_type")).as("frequent_crime_types"),
      concat_ws(", ", collect_list("offense_code")).as("frequent_crime_types_code"))
    .orderBy($"district")
  //println(crimeType.show(40, 19))

  val crime = dfCrime
    .select(coalesce(dfCrime("DISTRICT"), lit("null")).as("district"),
      $"MONTH"
      //date_trunc("Month", dfCrime("OCCURRED_ON_DATE")).as("date_month")
    )
    //.where($"district" === "A1")
   // .groupBy($"district", $"date_month")
    .groupBy($"district", $"MONTH")
    .agg(count("*").as("crimes_total"))
    //.orderBy($"district", $"date_month")
    .orderBy($"district", $"MONTH")

  println(crime.show(4000))

  val mediana = crime
    .groupBy($"district")
    .agg(callUDF("percentile_approx", $"crimes_total", lit(0.5)).as("crimes_monthly"),
      sum($"crimes_total").as("crimes_total"))
    .orderBy($"district")
    .select($"district",
      $"crimes_total",
      $"crimes_monthly"
    )
  println(mediana.show())

  val df: Dataset[bostoncrimes] = crimeType
    .join(mediana, crimeType("district") === mediana("district"))
    .select(crime("district"),
      $"crimes_total",
      $"crimes_monthly",
      $"frequent_crime_types",
      $"lat",
      $"lng"
    ).as[bostoncrimes]

  //println(df.show(14, false))
  df.coalesce(1).write.mode(SaveMode.Overwrite).parquet(args(2))
}
