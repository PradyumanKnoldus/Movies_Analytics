package dataFrames

import sparkSession.SparkSessions._
import org.apache.spark.sql.functions._

object Prepare_Movies_Data {
  val moviesDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("delimiter", "::")
    .option("ignoreTrailingWhiteSpace", "true")
    .load("/home/knoldus/IdeaProjects/spark-github-assignment/github-assignment/src/main/resources/Movielens/movies.dat")
    .toDF("MovieID", "Title", "Genres")
//  moviesData.show(false)

  val flattenData = moviesDF.withColumn("Year", regexp_extract(col("Title"), "(?<=\\()\\d{4}(?=[^\\)]*\\))", 0))
//  flattenData.show(false)

  val trimmedData = flattenData.withColumn("Year", trim(col("Year")))
//  trimmedData.show(false)

  val moviesData = trimmedData.withColumn("Title", regexp_replace(col("Title"), "\\s*\\(\\d{4}\\)", ""))
    .withColumn("Genres", split(col("Genres"), "\\|"))
    .select("MovieID", "Title", "Genres", "Year")

//  moviesData.show(false)
}
