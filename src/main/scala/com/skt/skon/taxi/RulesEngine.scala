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
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.KeyedStateFunction
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.codehaus.janino.ExpressionEvaluator
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse

import scala.util.Try

object RulesEngine extends LazyLogging {
  case class TaxiFinder(condition: String)
  case class TaxiRideStatus(rideId: Long, taxiId: Long, driverId: Long, isStart: Boolean, longitude: Float, latitude: Float, timestamp: Long, passengerCount: Short)

  object To {
    def taxiRideStatus(ride: TaxiRide): TaxiRideStatus = {
      if (ride.isStart) TaxiRideStatus(ride.rideId, ride.taxiId, ride.driverId, ride.isStart, ride.startLon, ride.startLat, ride.startTime.getMillis, ride.passengerCnt)
      else TaxiRideStatus(ride.rideId, ride.taxiId, ride.driverId, ride.isStart, ride.endLon, ride.endLat, ride.endTime.getMillis, ride.passengerCnt)
    }
  }

  def main(args: Array[String]): Unit = {
    val servingSpeedFactor = 60   // events of 1 minutes are served in 1 second

    val consumerProperties = new Properties()
    consumerProperties.setProperty("bootstrap.servers", "localhost:9092")
    consumerProperties.setProperty("group.id", "taxi-query")

    val producerProperties = new Properties()
    producerProperties.setProperty("bootstrap.servers", "localhost:9092")

    // set up the streaming execution environment
    val env = TaxiExecutionEnvironment.getExecutionEnvironment()

    val rideStream = env
      .addSource(new CheckpointedTaxiRideSource(TaxiExecutionEnvironment.rideSourcePath, servingSpeedFactor))
      .keyBy(_.taxiId)

    val requestStream = env
      .addSource({
        val consumer = new FlinkKafkaConsumer[String]("taxi-request", new SimpleStringSchema, consumerProperties)
        consumer.setStartFromLatest()
        consumer
      })
      .assignTimestampsAndWatermarks(new KeyBoardInputTimestampAndWatermarks[String])
      .flatMap(new FlatMapFunction[String, TaxiFinder] {
        override def flatMap(request: String, out: Collector[TaxiFinder]): Unit = {
          Try {
            implicit val formats: DefaultFormats.type = DefaultFormats
            out.collect(parse(request).extract[TaxiFinder])
          }.recover {
            case e: Exception => logger.debug(e.getMessage)
            case _ => logger.debug("Unkonwn error is occurred.")
          }
        }
      })
      .broadcast(new MapStateDescriptor[Long, Long]("dummy", classOf[Long], classOf[Long]))

    val distanceStream = rideStream
      .connect(requestStream)
      .process(new TaxiFinderProcessFunction)
      .setParallelism(4)

    TaxiExecutionEnvironment.print(distanceStream, "taxi-response", new TaxiSchema[TaxiRideStatus], producerProperties)

    env.execute("Rules Engine")
  }

  class TaxiFinderProcessFunction extends KeyedBroadcastProcessFunction[Long, TaxiRide, TaxiFinder, TaxiRideStatus] {
    private val rideStateDiscriptor =  new ValueStateDescriptor[TaxiRide]("taxi-ride", classOf[TaxiRide])
    private lazy val rideState = getRuntimeContext.getState(rideStateDiscriptor)

    override def processElement(ride: TaxiRide, ctx: KeyedBroadcastProcessFunction[Long, TaxiRide, TaxiFinder, TaxiRideStatus]#ReadOnlyContext, out: Collector[TaxiRideStatus]): Unit = {
      rideState.update(ride)
    }

    override def processBroadcastElement(value: TaxiFinder, ctx: KeyedBroadcastProcessFunction[Long, TaxiRide, TaxiFinder, TaxiRideStatus]#Context, out: Collector[TaxiRideStatus]): Unit = {
      ctx.applyToKeyedState(rideStateDiscriptor, new KeyedStateFunction[Long, ValueState[TaxiRide]] {
        val a: Array[Class[_]] = Array(classOf[Long])
        override def process(key: Long, rideState: ValueState[TaxiRide]): Unit = {
          val ride = rideState.value
          val watermark = ctx.currentWatermark

          if (ride !=null) {
            Try {
              val ee = new ExpressionEvaluator()
              ee.setParameters(Array("watermark", "ride"), Array(classOf[Long], classOf[TaxiRide]).asInstanceOf[Array[Class[_]]])
              ee.setExpressionType(classOf[Boolean])
              ee.cook(value.condition.toString)
              val result = ee.evaluate(Array(watermark, ride).asInstanceOf[Array[AnyRef]]).asInstanceOf[Boolean]
              if (result) out.collect(To.taxiRideStatus(ride))
            }.recover {
              case e: Exception => logger.debug(e.getMessage)
              case _ => logger.error("Unknown error is occurred.")
            }
          }
        }
      })
    }
  }
}
