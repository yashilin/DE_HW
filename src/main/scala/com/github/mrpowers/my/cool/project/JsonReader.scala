package com.github.mrpowers.my.cool.project
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.JsonMethods._
import org.json4s._
import org.json4s.jackson.Serialization

case class Users(
                  id: Option[Int],
                  country: Option[String],
                  points: Option[Int],
                  price: Option[Double],
                  title: Option[String],
                  variety: Option[String],
                  winery: Option[String])

object JsonReader extends App {
  implicit val formats = {
    Serialization.formats(FullTypeHints(List(classOf[Users])))
  }
 // System.setProperty("hadoop.home.dir", "D:/winutils")
  val spark = SparkSession.builder().master("local").getOrCreate()
  val sc = spark.sparkContext
  val filename = args(0)
  val decodedUser = sc.textFile(filename)
  decodedUser.foreach(i => println(parse(i).extract[Users]))
 }
