package it.devday.sparkplayground

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

object SimpleApp {
  def main(args: Array[String]) {

    val session = SparkSessionFactory.session
    val service = new FlightsService
    val flightDF: DataFrame = service.extract("flights2.csv", session)

    import session.implicits._
    val flightDS:Dataset[Flight] = flightDF.as[Flight]

    //DS and DF: FILTER
//    val cancelled = flightDS
//      .filter( a=> a.cancelled == 1)
//        .count()

//    val cancelled = flightDF
//        .filter($"cancelled" === 1)
//        .count()
    //    println(s"CANCELLED FLIGHT $cancelled")

    //ONLY DATAFRAME
//    flightDF.
//      groupBy(flightDF("ORIGIN_AIRPORT"))
//        .agg(
//          sum("CANCELLED").as("CANC_COUNT")
//        )
//        .select("ORIGIN_AIRPORT", "CANC_COUNT")
//        .agg(
//          first("ORIGIN_AIRPORT"),
//          max("CANC_COUNT")
//        )
//        .show(truncate = false)

    //UDF
//    flightDF
//        .withColumn("FLIGHT_DATE_STR", concat('DAY, lit("-"), 'MONTH, lit("-"), 'YEAR))
//        .withColumn("FLIGHT_DATE", to_date('FLIGHT_DATE_STR, "dd-mm-yyyy"))
//        .drop("FLIGHT_DATE_STR")
//      .select("ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "FLIGHT_DATE")
//        .show(truncate = false)

//    flightDS
//        .map{
//          f => FlightWithDate(Timestamp.valueOf(s"${f.year}-${f.month}-${f.day} 00:00:00"), f.flight_number, f.origin_airport, f.destination_airport, f.cancelled)
//        }
//        .show(truncate = false)

    SparkSessionFactory.session.close()
  }
}
