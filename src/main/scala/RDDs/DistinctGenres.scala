package RDDs

import dataFrames.Prepare_Movies_Data.moviesData
import org.apache.spark.sql.functions.{col, explode}
import sparkSession.SparkSessions.spark

object DistinctGenres extends App {

  val moviesRDD = spark.sparkContext.textFile("/home/knoldus/IdeaProjects/spark-github-assignment/github-assignment/src/main/resources/Movielens/movies.dat")

  val movieGenres = moviesRDD.map(line => line.split("::")(2))
  val allGenres = movieGenres.flatMap(line => line.split('|')).distinct

  allGenres.foreach(println)

  //------------------------------------------------------------------------------------------------------

  val genresAvailable = moviesData.withColumn("Genres", explode(col("Genres")))

  genresAvailable.select("Genres").distinct.show(false)

}
