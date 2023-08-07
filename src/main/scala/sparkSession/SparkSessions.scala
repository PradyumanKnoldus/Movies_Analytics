package sparkSession

import org.apache.spark.sql.SparkSession

object SparkSessions {
  val spark = SparkSession.builder()
    .appName("DataFrames")
    .master("local[1]")
    .getOrCreate()
}
