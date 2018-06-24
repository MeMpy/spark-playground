package it.devday.sparkplayground

import java.sql.Timestamp

/***
  * YEARYear of the Flight Trip
  * MONTHMonth of the Flight Trip
  * DAYDay of the Flight Trip
  * DAY_OF_WEEKDay of week of the Flight Trip
  * AIRLINEAirline Identifier
  * FLIGHT_NUMBERFlight Identifier
  * TAIL_NUMBERAircraft Identifier
  * ORIGIN_AIRPORTStarting Airport
  * DESTINATION_AIRPORTDestination Airport
  * SCHEDULED_DEPARTUREPlanned Departure Time
  * DEPARTURE_TIMEWHEEL_OFF - TAXI_OUT
  * DEPARTURE_DELAYTotal Delay on Depature
  * TAXI_OUTThe time duration elapsed between departure from the origin airport gate and wheels off
  * WHEELS_OFFThe time point that the aircraft's wheels leave the ground
  * SCHEDULED_TIMEPlanned time amount needed for the flight trip
  * ELAPSED_TIMEAIR_TIME+TAXI_IN+TAXI_OUT
  * AIR_TIMEThe time duration between wheels_off and wheels_on time
  * DISTANCEDistance between two airports
  * WHEELS_ONThe time point that the aircraft's wheels touch on the ground
  * TAXI_INThe time duration elapsed between wheels-on and gate arrival at the destination airport
  * SCHEDULED_ARRIVALPlanned arrival time
  * ARRIVAL_TIMEWHEELS_ON+TAXI_IN
  * ARRIVAL_DELAYARRIVAL_TIME-SCHEDULED_ARRIVAL
  * DIVERTEDAircraft landed on airport that out of schedule
  * CANCELLEDFlight Cancelled (1 = cancelled)
  * CANCELLATION_REASONReason for Cancellation of flight: A - Airline/Carrier; B - Weather; C - National Air System; D - Security
  * AIR_SYSTEM_DELAYDelay caused by air system
  * SECURITY_DELAYDelay caused by security
  * AIRLINE_DELAYDelay caused by the airline
  * LATE_AIRCRAFT_DELAYDelay caused by aircraft
  * WEATHER_DELAYDelay caused by weather
  */

case class Flight(year:Int, month:Int, day:Int, flight_number:Int, origin_airport:String, destination_airport:String, cancelled:Int)

case class FlightWithDate(date:Timestamp, flight_number:Int, origin_airport:String, destination_airport:String, cancelled:Int)