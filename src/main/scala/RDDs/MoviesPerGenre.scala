package RDDs

import dataFrames.Prepare_Movies_Data.moviesData
import sparkSession.SparkSessions.spark
import org.apache.spark.sql.functions.{col, count, explode}

object MoviesPerGenre extends App {
  val movieRDD = spark.sparkContext.textFile("/home/knoldus/IdeaProjects/spark-github-assignment/github-assignment/src/main/resources/Movielens/movies.dat")
  val movieGenres = movieRDD.map(line => line.split("::")(2))
  val genresAvailable = movieGenres.flatMap(line => line.split('|'))
  val pairedGenres = genresAvailable.map(x => (x, 1))
  val moviesPerGenre = pairedGenres.reduceByKey(_ + _)

  moviesPerGenre.foreach(println)

  //-----------------------------------------------------------------------------------------------------

  val flattenDF = moviesData.withColumn("Genres", explode(col("Genres")))

  flattenDF.groupBy("Genres").agg(count("Title")).show(false)
}
