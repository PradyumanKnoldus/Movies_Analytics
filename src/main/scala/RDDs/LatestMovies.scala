package RDDs

import dataFrames.Prepare_Movies_Data.moviesData
import org.apache.spark.sql.functions.col
import sparkSession.SparkSessions.spark

object LatestMovies extends App {
  val moviesRDD = spark.sparkContext.textFile("/home/knoldus/IdeaProjects/spark-github-assignment/github-assignment/src/main/resources/Movielens/movies.dat")
  val movieTitle = moviesRDD.map(line => line.split("::")(1))
  val movieYear = movieTitle.map(line => line.substring(line.lastIndexOf("(")+1, line.lastIndexOf(")")))
  val latestYear = movieYear.max
  val latestMovies = movieTitle.filter(line => line.contains(s"(${latestYear})"))

  latestMovies.foreach(println)

  //------------------------------------------------------------------------------------------------------------

  val latestReleasedMovies = moviesData.select("*").orderBy(col("Year").desc)

  latestReleasedMovies.show(false)
}
