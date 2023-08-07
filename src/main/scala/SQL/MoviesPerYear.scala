package SQL

import dataFrames.Prepare_Movies_Data.moviesData
import sparkSession.SparkSessions.spark

object MoviesPerYear extends App {
  moviesData.createOrReplaceTempView("Movies")

  spark.sql(""" SELECT Year, count(Title) FROM Movies GROUP BY Year """).show(false)
}
