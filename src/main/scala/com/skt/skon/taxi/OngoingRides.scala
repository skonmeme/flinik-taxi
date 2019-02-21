package com.skt.skon.taxi

import java.util.Properties

import com.skt.skon.taxi.datatypes.TaxiRide
import com.skt.skon.taxi.schemas.TaxiSchema
import com.skt.skon.taxi.sources.CheckpointedTaxiRideSource
import com.skt.skon.taxi.utils.TaxiExecutionEnvironment
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.joda.time.DateTime
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse

import scala.util.Try

case class TaxiRequest(ago: Long)
case class TaxiRideStatus(rideId: Long, startTime: Long, endTime: Option[Long])
case class TaxiStatusAt(from: Long, to: Long, ongoingRide: Long)

object OngoingRides extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val rideStateDescriptor = new MapStateDescriptor[Long, TaxiRideStatus]("taxi-ride", classOf[Long], classOf[TaxiRideStatus])

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
      .keyBy("rideId")
      .broadcast(rideStateDescriptor)

    val requestStream = env
      .addSource({
        val consumer = new FlinkKafkaConsumer[String]("taxi-request", new SimpleStringSchema(), consumerProperties)
        consumer.setStartFromLatest()
        consumer
      })
      .flatMap(new FlatMapFunction[String, TaxiRequest] {
        override def flatMap(call: String, out: Collector[TaxiRequest]): Unit = {
          try {
            implicit val formats: DefaultFormats.type = DefaultFormats
            out.collect(parse(call).extract[TaxiRequest])
          }
        }
      })

    val distanceStream = requestStream
      .connect(rideStream)
      .process(new OngoingRideProcessFunction)
      .setParallelism(4)

    TaxiExecutionEnvironment.print[TaxiStatusAt](distanceStream, "taxi-response", new TaxiSchema[TaxiStatusAt], producerProperties)

    env.execute("Ongoing Rides")
  }

  class OngoingRideProcessFunction extends KeyedBroadcastProcessFunction[Long, TaxiRequest, TaxiRide, TaxiStatusAt] {
    private val rideStateDescriptor = new MapStateDescriptor[Long, TaxiRideStatus]("taxi-ride", classOf[Long], classOf[TaxiRideStatus])

    override def processBroadcastElement(ride: TaxiRide, ctx: KeyedBroadcastProcessFunction[Long, TaxiRequest, TaxiRide, TaxiStatusAt]#Context, out: Collector[TaxiStatusAt]): Unit = {
      val rideStatus = ctx.getBroadcastState(rideStateDescriptor).get(ride.rideId)

      rideStatus match {
        case null if ride.isStart =>
          ctx.getBroadcastState(rideStateDescriptor)
            .put(ride.rideId, TaxiRideStatus(ride.rideId, ride.startTime.getMillis, None))
        case TaxiRideStatus(rideId, startTime, None) if !ride.isStart =>
          ctx.getBroadcastState(rideStateDescriptor)
            .put(rideId, TaxiRideStatus(rideId, startTime, Some(ride.endTime.getMillis)))
        case _ =>
      }
    }

    override def processElement(request: TaxiRequest, ctx: KeyedBroadcastProcessFunction[Long, TaxiRequest, TaxiRide, TaxiStatusAt]#ReadOnlyContext, out: Collector[TaxiStatusAt]): Unit = {
      val currentTime = ctx.timestamp
      var count = 0

      ctx.getBroadcastState(rideStateDescriptor).immutableEntries.forEach {
        _ match {
          case TaxiRideStatus(_, startTime, None) =>
            if (startTime < currentTime) count += 1
          case TaxiRideStatus(_, startTime, Some(endTime)) =>
            if (startTime < currentTime && endTime >= currentTime + request.ago * 1000L) count += 1
          case _ =>
        }
      }

      out.collect(TaxiStatusAt(currentTime - request.ago * 1000L, currentTime, count))
    }
  }
}
