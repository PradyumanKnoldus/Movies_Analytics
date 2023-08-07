package SQL

import dataFrames.Prepare_Ratings_Data.ratingsData
import sparkSession.SparkSessions.spark

object MoviesForEachRating extends App {
  ratingsData.createOrReplaceTempView("Ratings")

  spark.sql(" SELECT Rating, count(MovieID) FROM Ratings GROUP BY Rating ").show(false)
}
