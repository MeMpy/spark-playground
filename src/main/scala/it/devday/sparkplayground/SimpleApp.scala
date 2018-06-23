package it.devday.sparkplayground

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\winutils")
    val logFile = getClass.getResource("/test.txt") // Should be some file on your system
    val sparkConf:SparkConf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local")

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val logData = spark.read.textFile(logFile.getPath).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}
