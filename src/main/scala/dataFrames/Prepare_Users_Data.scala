package dataFrames

import sparkSession.SparkSessions._

object Prepare_Users_Data {
  val usersData = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("delimiter", "::")
    .option("ignoreTrailingWhiteSpace", "true")
    .load("/home/knoldus/IdeaProjects/spark-github-assignment/github-assignment/src/main/resources/Movielens/users.dat")
    .toDF("UserID", "Gender", "Age", "Occupation", "Zip-code")

//  usersData.show(false)
}
