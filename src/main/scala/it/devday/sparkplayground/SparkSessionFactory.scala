package it.devday.sparkplayground

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  private def initSession = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local")

    SparkSession.builder.config(sparkConf).getOrCreate()
  }

  lazy val session:SparkSession = initSession
}
