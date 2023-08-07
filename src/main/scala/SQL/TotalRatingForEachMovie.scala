package SQL

import dataFrames.Prepare_Ratings_Data.ratingsData
import sparkSession.SparkSessions.spark

object TotalRatingForEachMovie extends App {
  ratingsData.createOrReplaceTempView("Ratings")

  spark.sql(" SELECT MovieID, sum(Rating) FROM Ratings GROUP BY MovieID ORDER BY MovieID ASC ").show(false)
}
