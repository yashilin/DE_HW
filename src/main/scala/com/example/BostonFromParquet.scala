package com.example


import org.apache.spark.sql._

case class boston(district: Option[String],
                        crimes_total: Option[BigInt],
                        crimes_monthly: Option[BigInt],
                        frequent_crime_types: Option[String],
                        lat: Option[Double],
                        lng: Option[Double]
                       )

object BostonFromParquet extends App {
  val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._
  val sc = spark.sparkContext

  val dfCrime = spark.read.parquet("source/1/boston.parquet").as[boston]
  println(dfCrime.show(false))

}