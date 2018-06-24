package it.devday.sparkplayground

import org.apache.spark.sql.{DataFrame, SparkSession}

/***
  * https://www.kaggle.com/levaniz/machine-learning-analysis-of-flights-data/data
  */
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

  def load(flightDS: DataFrame, partitions:Int = 3): Unit = {
    val destPath = "D:\\Projects\\spark-playground\\flight_csv"
    flightDS
      .repartition(partitions)
      .write
      .format("csv")
      .option("header", "true")
      .save(destPath)
  }



}
