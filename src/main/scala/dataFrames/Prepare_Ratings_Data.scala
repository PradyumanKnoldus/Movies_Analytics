package dataFrames

import sparkSession.SparkSessions._

object Prepare_Ratings_Data  {
  val ratingsData = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("delimiter", "::")
    .option("ignoreTrailingWhiteSpace", "true")
    .load("/home/knoldus/IdeaProjects/spark-github-assignment/github-assignment/src/main/resources/Movielens/ratings.dat")
    .toDF("UserID", "MovieID", "Rating", "Timestamp")

//  ratingsData.show(false)

}
