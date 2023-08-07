package RDDs

import dataFrames.Prepare_Movies_Data.moviesData
import org.apache.spark.sql.functions._
import sparkSession.SparkSessions.spark

object StartingWithCharacter extends App {
  val movieRDD = spark.sparkContext.textFile("/home/knoldus/IdeaProjects/spark-github-assignment/github-assignment/src/main/resources/Movielens/movies.dat")
  val movieTitle = movieRDD.map(line => line.split("::")(1))
  val startingCharOfTitle = movieTitle.flatMap(line => line.split("")(0))
  val pairedTitleChar = startingCharOfTitle.map(x => (x, 1))
  val titlePerCharacter = pairedTitleChar.reduceByKey(_ + _).sortByKey()

  titlePerCharacter.foreach(println)

  //---------------------------------------------------------------------------------------------------------

  val titleStartingWith = moviesData.withColumn("Title", split(col("Title"), "")(0))
    .groupBy("Title").count().sort("Title")

  titleStartingWith.show(false)
}
