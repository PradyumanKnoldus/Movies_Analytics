package SQL

import dataFrames.Prepare_Ratings_Data.ratingsData
import sparkSession.SparkSessions.spark

object UserRatingEachMovie extends App {
  ratingsData.createOrReplaceTempView("Ratings")

  spark.sql(" SELECT MovieID, count(UserID) FROM Ratings GROUP BY MovieID ORDER BY MovieID ASC").show(false)
}
