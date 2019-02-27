package com.skt.skon.taxi

import java.util.Properties

import com.skt.skon.taxi.datatypes.TaxiRide
import com.skt.skon.taxi.schemas.TaxiSchema
import com.skt.skon.taxi.sources.CheckpointedTaxiRideSource
import com.skt.skon.taxi.time.KeyBoardInputTimestampAndWatermarks
import com.skt.skon.taxi.utils.TaxiExecutionEnvironment
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.KeyedStateFunction
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse

import scala.util.Try

case class TaxiRequest(during: Long)
case class TaxiOngoingRide(rideId: Long, taxiId: Long , driverId: Long, startTime: Long, longitude: Float, latitude: Float, passengerCount: Short)
case class TaxiOngoingStatus(at: Long, during: Long, ride: TaxiOngoingRide)

object OngoingRides extends LazyLogging {
  def main(args: Array[String]): Unit = {
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
      .keyBy(_.rideId)

    val requestStream = env
      .addSource({
        val consumer = new FlinkKafkaConsumer[String]("taxi-request", new SimpleStringSchema, consumerProperties)
        consumer.setStartFromLatest()
        consumer
      })
      .assignTimestampsAndWatermarks(new KeyBoardInputTimestampAndWatermarks[String])
      .flatMap(new FlatMapFunction[String, TaxiRequest] {
        override def flatMap(request: String, out: Collector[TaxiRequest]): Unit = {
          Try {
            implicit val formats: DefaultFormats.type = DefaultFormats
            out.collect(parse(request).extract[TaxiRequest])
          }.recover {
            case e: Exception => logger.debug(e.getMessage)
            case _ => logger.debug("Unkonwn error is occurred.")
          }
        }
      })
      .broadcast(new MapStateDescriptor[Long, Long]("dumy", classOf[Long], classOf[Long]))

    val distanceStream = rideStream
      .connect(requestStream)
      .process(new TaxiOngoingRideProcessFunction)
      .setParallelism(4)

    TaxiExecutionEnvironment.print[TaxiOngoingStatus](distanceStream, "taxi-response", new TaxiSchema[TaxiOngoingStatus], producerProperties)

    env.execute("Ongoing Rides")
  }

  class TaxiOngoingRideProcessFunction extends KeyedBroadcastProcessFunction[Long, TaxiRide, TaxiRequest, TaxiOngoingStatus] {
    private val taxiRideStateDescriptor = new ValueStateDescriptor("ongoing-ride", classOf[TaxiRide])
    private lazy val taxiRideState = getRuntimeContext.getState(taxiRideStateDescriptor)

    private object To {
      def taxiOngoingRide(ride: TaxiRide) =
        TaxiOngoingRide(ride.rideId, ride.taxiId, ride.driverId,
          ride.startTime.getMillis, ride.startLon, ride.startLat, ride.passengerCnt)
    }

    override def processElement(ride: TaxiRide, ctx: KeyedBroadcastProcessFunction[Long, TaxiRide, TaxiRequest, TaxiOngoingStatus]#ReadOnlyContext, out: Collector[TaxiOngoingStatus]): Unit = {
      val ongoingRide = taxiRideState.value
      if (ongoingRide == null && ride.isStart) taxiRideState.update(ride)
      else if (ongoingRide != null && !ride.isStart) taxiRideState.clear()
    }

    override def processBroadcastElement(request: TaxiRequest, ctx: KeyedBroadcastProcessFunction[Long, TaxiRide, TaxiRequest, TaxiOngoingStatus]#Context, out: Collector[TaxiOngoingStatus]): Unit = {
      val currentWaterMark = ctx.currentWatermark
      val during = request.during * 60 * 1000L

      ctx.applyToKeyedState(taxiRideStateDescriptor, new KeyedStateFunction[Long, ValueState[TaxiRide]] {
        override def process(key: Long, state: ValueState[TaxiRide]): Unit = {
          val ride = state.value
          if (ride != null && ride.startTime.getMillis <= currentWaterMark - during)
            out.collect(TaxiOngoingStatus(currentWaterMark, during, To.taxiOngoingRide(ride)))
        }
      })
    }
  }

  /*
  class OngoingRideProcessFunction2 extends BroadcastProcessFunction[TaxiRequest, TaxiRide, TaxiStatusAt] {
    private val rideStateDescriptor = new MapStateDescriptor[Long, TaxiRideStatus]("taxi-ride", classOf[Long], classOf[TaxiRideStatus])

    override def processBroadcastElement(ride: TaxiRide, ctx: BroadcastProcessFunction[TaxiRequest, TaxiRide, TaxiStatusAt]#Context, out: Collector[TaxiStatusAt]): Unit = {
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

    override def processElement(request: TaxiRequest, ctx: BroadcastProcessFunction[TaxiRequest, TaxiRide, TaxiStatusAt]#ReadOnlyContext, out: Collector[TaxiStatusAt]): Unit = {
      val currentTime = ctx.currentWatermark
      var count = 0

      val ago = if (request.ago > 0) request.ago else 0
      ctx.getBroadcastState(rideStateDescriptor).immutableEntries.forEach( rideStatus => {
        rideStatus.getValue match {
          case TaxiRideStatus(_, startTime, Some(endTime)) =>
            if (startTime < currentTime && endTime >= currentTime - ago * 60 * 1000L) count += 1
          case TaxiRideStatus(_, startTime, None) =>
            if (startTime < currentTime) count += 1
          case _ =>
        }
      })
      out.collect(TaxiStatusAt(currentTime - ago * 60 * 1000L, currentTime, count))
    }
  }
  */
}
