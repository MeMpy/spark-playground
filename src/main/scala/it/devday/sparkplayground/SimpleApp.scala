package it.devday.sparkplayground

import org.apache.spark.sql.{DataFrame, Dataset}

object SimpleApp {
  def main(args: Array[String]) {

    val session = SparkSessionFactory.session
    val service = new FlightsService
    val flightDF: DataFrame = service.extract("flights2.csv", session)

    import session.implicits._
    val flightDS:Dataset[Flight] = flightDF.as[Flight]

    SparkSessionFactory.session.close()
  }
}
