package SQL

import dataFrames.Prepare_Movies_Data.moviesData
import sparkSession.SparkSessions.spark

object OldestReleasedMovies extends App {
  moviesData.createOrReplaceTempView("Movies")

  spark.sql(""" SELECT * FROM Movies WHERE Year = ( SELECT min(Year) FROM Movies )""").show(false)
}
