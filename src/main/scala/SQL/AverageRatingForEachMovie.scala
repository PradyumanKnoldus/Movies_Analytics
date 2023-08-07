package SQL

import dataFrames.Prepare_Ratings_Data.ratingsData
import sparkSession.SparkSessions.spark

object AverageRatingForEachMovie extends App {
  ratingsData.createOrReplaceTempView("Ratings")

  spark.sql(" SELECT MovieID, avg(Rating) FROM Ratings GROUP BY MovieID ORDER BY MovieID ").show(false)
}
