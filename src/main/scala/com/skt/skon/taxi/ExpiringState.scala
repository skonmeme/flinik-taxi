package com.skt.skon.taxi

import java.util.Properties

import com.skt.skon.taxi.datatypes.{TaxiFare, TaxiRide}
import com.skt.skon.taxi.schemas.TaxiSchema
import com.skt.skon.taxi.sources.{CheckpointedTaxiFareSource, CheckpointedTaxiRideSource}
import com.skt.skon.taxi.utils.TaxiExecutionEnvironment
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization._

object ExpiringState {
  val soloRideOutputTag = new OutputTag[TaxiRide]("solo-ride")
  val soloFareOutputTag = new OutputTag[TaxiFare]("solo-fare")

  def main(args: Array[String]): Unit = {
    val servingSpeedFactor = 1800   // events of 30 minutes are served in 1 second

    // set up the streaming execution environment
    val env = TaxiExecutionEnvironment.getExecutionEnvironment()

    val rideStream = env
      .addSource(new CheckpointedTaxiRideSource(TaxiExecutionEnvironment.rideSourcePath, servingSpeedFactor))
      .filter(ride => ride.isStart && ride.driverId % 1000 != 0)
      .keyBy("rideId")

    val fareStream = env
      .addSource(new CheckpointedTaxiFareSource(TaxiExecutionEnvironment.fareSourcePath, servingSpeedFactor))
      .keyBy("rideId")

    val connectedStreams = rideStream
      .connect(fareStream)
      .process(new ParingProcessFunction)

    val producerProperties = new Properties()
    producerProperties.setProperty("bootstrap.servers", "localhost:9092")

    TaxiExecutionEnvironment.print[TaxiFare](connectedStreams.getSideOutput(soloFareOutputTag), "taxi-expiring", new TaxiSchema[TaxiFare], producerProperties)

    env.execute("Expiring State")
  }

  class ParingProcessFunction extends CoProcessFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)] {
    private lazy val rideState = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiRide]("taxiRide", classOf[TaxiRide]))

    private lazy val fareState = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiFare]("fareRide", classOf[TaxiFare]))

    override def processElement1(value: TaxiRide, ctx: CoProcessFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#Context, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      if (fareState.value == null) {
        rideState.update(value)
        ctx.timerService.registerEventTimeTimer(value.getEventTime)
      } else {
        val fare = fareState.value
        fareState.clear()
        out.collect((value, fare))
      }
    }

    override def processElement2(value: TaxiFare, ctx: CoProcessFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#Context, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      if (rideState.value == null) {
        fareState.update(value)
        ctx.timerService.registerEventTimeTimer(value.getEventTime)
      } else {
        val ride = rideState.value
        rideState.clear()
        out.collect((ride, value))
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]#OnTimerContext, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      val ride = rideState.value
      val fare = fareState.value

      if (ride != null && fare == null) {
        rideState.clear()
        ctx.output(soloRideOutputTag, ride)
      } else if (ride == null && fare != null) {
        fareState.clear()
        ctx.output(soloFareOutputTag, fare)
      }
    }
  }
}
