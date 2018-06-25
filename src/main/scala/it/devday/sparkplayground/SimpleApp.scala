package it.devday.sparkplayground

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

object SimpleApp {

  val session = SparkSessionFactory.session
  val service = new FlightsService

  import session.implicits._

  def main(args: Array[String]) {

    val flightDF: DataFrame = service.load("flights2.csv", session)

    val flightDS: Dataset[Flight] = flightDF.as[Flight]

//        service.save(flightDF)

    //    filterOnDataFrameAndOnDataSet(flightDF, flightDS)

    //    groupByOnlyDataframe(flightDF)

    //    addColumnInDataframeAndPureDataset(flightDF, flightDS)

//    addColumnWithUDF(flightDF)

//        typicalMistakeAndHowToAvoidIt(flightDF, flightDS)

    SparkSessionFactory.session.close()
  }

  private def filterOnDataFrameAndOnDataSet(flightDF: DataFrame, flightDS: Dataset[Flight]): Unit = {
    //DS and DF: FILTER
    val cancelled1 = flightDS
      .filter(a => a.cancelled == 1)
      .count()

    val cancelled2 = flightDF
      .filter('cancelled === 1)
      .count()

    println(s"CANCELLED FLIGHT $cancelled1")
    println(s"CANCELLED FLIGHT $cancelled2")
  }

  private def groupByOnlyDataframe(flightDF: DataFrame): Unit = {
    flightDF.
      groupBy(flightDF("ORIGIN_AIRPORT"))
      .agg(
        sum("CANCELLED").as("CANC_COUNT")
      )
      .select("ORIGIN_AIRPORT", "CANC_COUNT")
      .agg(
        first("ORIGIN_AIRPORT"),
        max("CANC_COUNT")
      )
      .show(truncate = false)
  }

  private def addColumnInDataframeAndPureDataset(flightDF: DataFrame, flightDS: Dataset[Flight]): Unit = {

    flightDF
      .withColumn("FLIGHT_DATE_STR", concat('DAY, lit("-"), 'MONTH, lit("-"), 'YEAR))
      .withColumn("FLIGHT_DATE", to_date('FLIGHT_DATE_STR, "dd-mm-yyyy"))
      .drop("FLIGHT_DATE_STR")
      .select("ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "FLIGHT_DATE")
      .show(truncate = false)

    flightDS
      .map {
        f => FlightWithDate(Timestamp.valueOf(s"${f.year}-${f.month}-${f.day} 00:00:00"), f.flight_number, f.origin_airport, f.destination_airport, f.cancelled)
      }
      .show(truncate = false)
  }

  private def addColumnWithUDF(flightDF:DataFrame){
    def convertMilesToKm(miles:Int) = miles * 1.609344

    val milesToKmUDF = udf{
      x:Int => convertMilesToKm(x)
    }

    flightDF
      .withColumn("DISTANCE_IN_KM", milesToKmUDF('DISTANCE))
      .show(truncate = false)
  }

  private def typicalMistakeAndHowToAvoidIt(flightDF: DataFrame, flightDS: Dataset[Flight]): Unit = {
    val airlines = service.load("airlines.csv", session)

    //TYPICAL MISTAKE
//    airlines.as[Airline].foreach { a =>
//      //null pointer
//      val airlineFlightDf = flightDS
//        //serialization error
//        //      val airlineFlightDf = service.extract("flights2.csv", session).as[Flight]
//        .filter(f => f.airline == a.iata_code)
//      airlineFlightDf.agg(sum("CANCELLED")).show()
//    }

    //JOIN
    flightDF
      .join(airlines, flightDF("AIRLINE") === airlines("IATA_CODE"))
      .groupBy('IATA_CODE)
      .agg(
        sum("CANCELLED").as("TOTAL_CANCELLATIONS")
      )
      .show(truncate = false)
  }
}
