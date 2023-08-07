package SQL

import dataFrames.Prepare_Movies_Data.moviesData
import dataFrames.Prepare_Ratings_Data.ratingsData
import dataFrames.Prepare_Users_Data.usersData
import sparkSession.SparkSessions.spark

object SaveAsTable extends App {
  moviesData.createOrReplaceTempView("Movies")
  ratingsData.createOrReplaceTempView("Ratings")
  usersData.createOrReplaceTempView("Users")

  spark.sql("DROP DATABASE IF EXISTS SparkSQL CASCADE")
  spark.sql("CREATE DATABASE SparkSQL")

  spark.sql(" SELECT * FROM Movies").write.mode("overwrite").saveAsTable("Movies_Data")
  spark.sql(" SELECT * FROM Ratings").write.mode("overwrite").saveAsTable("Ratings_Data")
  spark.sql(" SELECT * FROM Users").write.mode("overwrite").saveAsTable("Users_Data")
}
