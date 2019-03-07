package com.skt.skon.taxi

import java.util.Properties

import com.skt.skon.taxi.schemas.TaxiSchema
import com.skt.skon.taxi.sources.CheckpointedTaxiRideSource
import com.skt.skon.taxi.utils.{GeoUtils, TaxiExecutionEnvironment}
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object PopularPlaces extends LazyLogging {

  case class TaxiPlace(cellId: Int, isStart: Boolean, count: Long)

  def main(args: Array[String]): Unit = {
    val servingSpeedFactor = 60 // events of 1 minutes are served in 1 second

    val consumerProperties = new Properties()
    consumerProperties.setProperty("bootstrap.servers", "localhost:9092")
    consumerProperties.setProperty("group.id", "taxi-query")

    val producerProperties = new Properties()
    producerProperties.setProperty("bootstrap.servers", "localhost:9092")

    // set up the streaming execution environment
    val env = TaxiExecutionEnvironment.getExecutionEnvironment()

    val rideStream = env
      .addSource(new CheckpointedTaxiRideSource(TaxiExecutionEnvironment.rideSourcePath, servingSpeedFactor))
      .filter(ride => GeoUtils.isInNYC(ride.startLon, ride.startLat) && GeoUtils.isInNYC(ride.endLon, ride.endLat))
      .map(ride =>
        if (ride.isStart) TaxiPlace(GeoUtils.mapToGridCell(ride.startLon, ride.startLat), ride.isStart, 1L)
        else TaxiPlace(GeoUtils.mapToGridCell(ride.endLon, ride.endLat), ride.isStart, 1L))
      .keyBy(place => (place.cellId, place.isStart))
      .window(TumblingEventTimeWindows.of(Time.minutes(10L), Time.minutes(1L)))
      .sum(3)

    val placeStream = rideStream
      .filter(place => place.count >= 20L)

    TaxiExecutionEnvironment.print(placeStream, "taxi-response", new TaxiSchema[TaxiPlace], producerProperties)

    env.execute("Popular Places")
  }
}
