package it.devday.sparkplayground

import org.apache.spark.sql.{DataFrame, SparkSession}

class FlightsService {

  def extract(filename:String, spark: SparkSession):DataFrame = {
    val flightsCsv = getClass.getResource(s"/$filename") // Should be some file on your system
    val flightDS = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(flightsCsv.getPath) //.cache()
    flightDS
  }

  def load(flightDS: DataFrame): Unit = {
    val destPath = "D:\\Projects\\spark-playground\\flight_csv"
    flightDS
      .repartition(3)
      .write
      .format("csv")
      .option("header", "true")
      .save(destPath)
  }



}
