package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Analyse {



  def main(args: Array[String]): Unit = {

    val PATH_TO_FILES = "data_analyse/src/main/resources/*"
  //  val PATH_TO_FILES_2 = "data_analyse/src/main/resources/NewRelease.csv"
    val spark = SparkSession
      .builder()
      .appName("Analyse")
      .master("local[*]")
      .getOrCreate()


    println("**************** Read Files ***********************")

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .csv(PATH_TO_FILES)
/*
    val df2 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .csv(PATH_TO_FILES_2)
*/
    df.printSchema
    println("**************** SCHEMA  ***********************")
    println("Count of row " + df.count())

   // Les albums les plus populaires
   val df_populaire = df.select("album_name","popularity").sort(desc("popularity"))
    df_populaire.show()


    df_populaire.write.csv("data_analyse/src/main/resources/data.csv")
   /**


    // Les artistes et leurs nombres d'albums.
      val df_nb_albums = df.groupBy("artist_names","album_name").count()

    df_nb_albums.show(100)

    // Les types d'albums sortis par artiste
     // val df_album_type = df.groupBy("album_type","artiste_names").pivot(max("album_type")).
     // df_album_type.show(7)


    val df_top_5 = df.groupBy(col("popularity")).count.sort(desc("count")).limit(5)
    df_top_5.show()


    /* Album et singles sortis à partir de  2020 */
    println("**************** Releases dates  ***********************")
    val df_datesorti = df.filter(to_date(df("release_date"),"MM/dd/yyyy").geq(lit("2017-01-01")))
    df_datesorti.show(5)


    /* Album et singles sortis entre deux dates données */
    val df_date = df.filter(to_date(df("date"),"MM/dd/yyyy").between("2019-01-01","2020-01-01"))
    df_date.show(5)

    **/
      }
}
