package it.devday.sparkplayground

import java.sql.Timestamp

/***
  * https://www.kaggle.com/levaniz/machine-learning-analysis-of-flights-data/data
  *
  * YEARY ear of the Flight Trip
  * MONTH Month of the Flight Trip
  * DAY Day of the Flight Trip
  * DAY_OF_WEEK Day of week of the Flight Trip
  * AIRLINE Airline Identifier
  * FLIGHT_NUMBER Flight Identifier
  * TAIL_NUMBER Aircraft Identifier
  * ORIGIN_AIRPORT Starting Airport
  * DESTINATION_AIRPORT Destination Airport
  * SCHEDULED_DEPARTURE Planned Departure Time
  * DEPARTURE_TIME WHEEL_OFF - TAXI_OUT
  * DEPARTURE_DELAY Total Delay on Depature
  * TAXI_OUT The time duration elapsed between departure from the origin airport gate and wheels off
  * WHEELS_OFF The time point that the aircraft's wheels leave the ground
  * SCHEDULED_TIME Planned time amount needed for the flight trip
  * ELAPSED_TIME AIR_TIME+TAXI_IN+TAXI_OUT
  * AIR_TIME The time duration between wheels_off and wheels_on time
  * DISTANCE Distance between two airports
  * WHEELS_ON The time point that the aircraft's wheels touch on the ground
  * TAXI_IN The time duration elapsed between wheels-on and gate arrival at the destination airport
  * SCHEDULED_ARRIVAL Planned arrival time
  * ARRIVAL_TIME WHEELS_ON+TAXI_IN
  * ARRIVAL_DELAY ARRIVAL_TIME-SCHEDULED_ARRIVAL
  * DIVERTED Aircraft landed on airport that out of schedule
  * CANCELLED Flight Cancelled (1 = cancelled)
  * CANCELLATION_REASON Reason for Cancellation of flight: A - Airline/Carrier; B - Weather; C - National Air System; D - Security
  * AIR_SYSTEM_DELAY Delay caused by air system
  * SECURITY_DELAY Delay caused by security
  * AIRLINE_DELAY Delay caused by the airline
  * LATE_AIRCRAFT_DELAY Delay caused by aircraft
  * WEATHER_DELAY Delay caused by weather
  */

case class Flight(year:Int, month:Int, day:Int, airline:String, flight_number:Int, origin_airport:String, destination_airport:String, cancelled:Int)

case class FlightWithDate(date:Timestamp, flight_number:Int, origin_airport:String, destination_airport:String, cancelled:Int)

case class Airline(iata_code:String, airline:String)