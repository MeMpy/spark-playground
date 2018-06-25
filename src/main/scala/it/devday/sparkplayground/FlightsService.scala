package it.devday.sparkplayground

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


class FlightsService {

  val base_dir = "/Users/rosario/other_project/spark-playground/flight_csv"

  def load(filename:String, spark: SparkSession):DataFrame = {
    val flightsCsv = s"/Users/rosario/other_project/spark-playground/flight_csv/$filename" // Should be some file on your system
    val flightDS = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(flightsCsv) //.cache()
    flightDS
  }

  def save(flightDS: DataFrame, partitions:Int = 3): Unit = {
    val destPath = "/Users/rosario/other_project/spark-playground/flight_csv_generated"
    flightDS
      .repartition(partitions)
      .write
      .format("csv")
      .option("header", "true")
      .save(destPath)
  }
}
