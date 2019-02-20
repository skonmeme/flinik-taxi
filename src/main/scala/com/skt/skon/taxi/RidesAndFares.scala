package com.skt.skon.taxi

import java.util.Properties

import com.skt.skon.taxi.datatypes.{TaxiFare, TaxiRide}
import com.skt.skon.taxi.schemas.TaxiSchema
import com.skt.skon.taxi.sources.{CheckpointedTaxiFareSource, CheckpointedTaxiRideSource}
import com.skt.skon.taxi.utils.TaxiExecutionEnvironment
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RidesAndFares {
  def main(args: Array[String]): Unit = {
    val servingSpeedFactor = 600   // events of 10 minutes are served in 1 second

    // set up the streaming execution environment
    val env = TaxiExecutionEnvironment.getExecutionEnvironment()

    val rideStream = env
      .addSource(new CheckpointedTaxiRideSource(TaxiExecutionEnvironment.rideSourcePath, servingSpeedFactor))
      .filter(ride => ride.isStart)
      .keyBy("rideId")
    val fareStream = env
      .addSource(new CheckpointedTaxiFareSource(TaxiExecutionEnvironment.fareSourcePath, servingSpeedFactor))
      .keyBy("rideId")

    val pairedStream = rideStream
      .connect(fareStream)
      .flatMap(new RideFareFlatMap)

    val producerProperties = new Properties()
    producerProperties.setProperty("bootstrap.servers", "localhost:9092")

    TaxiExecutionEnvironment.print(pairedStream, "taxi-expiring", new TaxiSchema[(TaxiRide, TaxiFare)], producerProperties)

    env.execute("Rides and Fares")
  }

  class RideFareFlatMap extends RichCoFlatMapFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)] {
    private lazy val rideState = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiRide]("taxiRide", classOf[TaxiRide]))

    private lazy val fareState = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiFare]("fareRide", classOf[TaxiFare]))

    override def flatMap1(value: TaxiRide, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      if (fareState.value == null) {
        rideState.update(value)
      } else {
        val fare = fareState.value
        fareState.clear()
        out.collect((value, fare))
      }
    }

    override def flatMap2(value: TaxiFare, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      if (rideState.value == null) {
        fareState.update(value)
      } else {
        val ride = rideState.value
        rideState.clear()
        out.collect((ride, value))
      }
    }
  }
}