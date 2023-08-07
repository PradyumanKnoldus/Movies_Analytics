package RDDs

import dataFrames.Prepare_Movies_Data.moviesDF
import dataFrames.Prepare_Ratings_Data.ratingsData
import org.apache.spark.sql.functions.col
import sparkSession.SparkSessions._

object MostViewedMovies extends App {
  val ratingsRDD = spark.sparkContext.textFile("/home/knoldus/IdeaProjects/spark-github-assignment/github-assignment/src/main/resources/Movielens/ratings.dat")
  val moviesOccurred = ratingsRDD.map(line => line.split("::")(1))
  val moviesPaired = moviesOccurred.map(movie => (movie,1))
  val moviesCount = moviesPaired.reduceByKey(_ + _).sortBy(_._2, ascending = false)

  val top10Movies = moviesCount.take(10)

  val top10MoviesRDD = spark.sparkContext.parallelize(top10Movies)

  val moviesRDD = spark.sparkContext.textFile("/home/knoldus/IdeaProjects/spark-github-assignment/github-assignment/src/main/resources/Movielens/movies.dat")
    .map(line => (line.split("::")(0), line.split("::")(1)))

  moviesRDD.join(top10MoviesRDD).foreach(println)

  //----------------------------------------------------------------------------------------------------------------

  val movieRatings = ratingsData.groupBy("MovieID").count.orderBy(col("count").desc).limit(10)

  val movieTitles = moviesDF.select("MovieID", "Title")

  movieRatings.join(movieTitles, "MovieID", "Left").show(false)

}
