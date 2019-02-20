package com.skt.skon.taxi

import java.util.Properties

import com.skt.skon.taxi.datatypes.TaxiRide
import com.skt.skon.taxi.schemas.TaxiSchema
import com.skt.skon.taxi.sources.CheckpointedTaxiRideSource
import com.skt.skon.taxi.utils.TaxiExecutionEnvironment
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.util.Try

case class TaxiCall(userId: Long, timestamp: Long, longitude: Double, latitude: Double)
case class TaxiLocation(taxiId: Long, timestamp: Long, logitude: Double, latitude: Double)
case class TaxiDistance(call: TaxiCall, taxi: TaxiLocation, distance: Double)

object NearestTaxi {
  def main(args: Array[String]): Unit = {
    val rideStateDescriptor = new MapStateDescriptor[Long, TaxiRide]("taxi-ride", classOf[Long], classOf[TaxiRide])

    val servingSpeedFactor = 60   // events of 1 minutes are served in 1 second

    val consumerProperties = new Properties()
    consumerProperties.setProperty("bootstrap.servers", "localhost:9092")
    consumerProperties.setProperty("group.id", "taxi-call")

    val producerProperties = new Properties()
    producerProperties.setProperty("bootstrap.servers", "localhost:9092")

    // set up the streaming execution environment
    val env = TaxiExecutionEnvironment.getExecutionEnvironment()

    val rideStream = env
      .addSource(new CheckpointedTaxiRideSource(TaxiExecutionEnvironment.rideSourcePath, servingSpeedFactor))
      .filter(!_.isStart)
      .broadcast(rideStateDescriptor)

    val callStream = env
      .addSource({
        val consumer = new FlinkKafkaConsumer[String]("taxi-call", new SimpleStringSchema(), consumerProperties)
        consumer.setStartFromLatest()
        consumer
      })
      .map(call => {
        Try {
          implicit val formats: DefaultFormats.type = DefaultFormats
          parse(call).extract[TaxiCall]
        }.toOption
      })

    val distanceStream = callStream
      .connect(rideStream)
      .process(new DistanceProcessFunction)
      .setParallelism(4)

    TaxiExecutionEnvironment.print[TaxiDistance](distanceStream, "taxi-distance", new TaxiSchema[TaxiDistance], producerProperties)

    env.execute("Neareat Taxi")
  }

  class DistanceProcessFunction(keepFor: Long = 60 * 60 * 1000L) extends BroadcastProcessFunction[Option[TaxiCall], TaxiRide, TaxiDistance] {
    private val rideStateDescriptor = new MapStateDescriptor[Long, TaxiRide]("taxi-ride", classOf[Long], classOf[TaxiRide])

    override def processBroadcastElement(ride: TaxiRide, ctx: BroadcastProcessFunction[Option[TaxiCall], TaxiRide, TaxiDistance]#Context, out: Collector[TaxiDistance]): Unit = {
      ctx.getBroadcastState(rideStateDescriptor).put(ride.taxiId, ride)
    }

    override def processElement(value: Option[TaxiCall], ctx: BroadcastProcessFunction[Option[TaxiCall], TaxiRide, TaxiDistance]#ReadOnlyContext, out: Collector[TaxiDistance]): Unit = {
      var nearestTaxi: TaxiDistance = TaxiDistance(TaxiCall(0, 0, 0, 0), TaxiLocation(0, 0, 0, 0), Double.MaxValue)
      var found = false

      value match {
        case Some(call) =>
          ctx.getBroadcastState(rideStateDescriptor).immutableEntries.forEach(rideState => {
            val ride = rideState.getValue
            val distance = ride.getEuclideanDistance(call.longitude, call.latitude)
            if (distance <= nearestTaxi.distance) {
              found = true
              nearestTaxi = TaxiDistance(call, TaxiLocation(ride.taxiId, ride.endTime.getMillis, ride.endLon, ride.endLat), distance)
            }
          })
          if (found) out.collect(nearestTaxi)
        case None =>
      }
    }

    override def open(parameters: Configuration): Unit = {
      val ttlConfig = StateTtlConfig
        .newBuilder(Time.milliseconds(keepFor))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .build

      rideStateDescriptor.enableTimeToLive(ttlConfig)
    }
  }
}