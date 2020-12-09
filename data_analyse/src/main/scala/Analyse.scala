package main.scala

import org.apache.spark.sql.SparkSession

object Analyse {



  def main(args: Array[String]): Unit = {

    val PATH_TO_FILES = "data_analyse/src/main/resources/*"
    val spark = SparkSession
      .builder()
      .appName("Analyse")
      .master("local[*]")
      .getOrCreate()


    println("**************** Read File ***********************")

    val df = spark.read.option("header", "true").option("inferSchema","true")
      .csv(PATH_TO_FILES)

    println(df.count())
  }
}
