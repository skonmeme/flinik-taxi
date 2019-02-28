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
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.codehaus.janino.ExpressionEvaluator
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse

import scala.util.Try

object RulesEngine extends LazyLogging {
  case class TaxiFinder(condition: String)

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

    TaxiExecutionEnvironment.print(distanceStream, "taxi-response", new TaxiSchema[TaxiRide], producerProperties)

    env.execute("Rules Engine")
  }

  class TaxiFinderProcessFunction extends KeyedBroadcastProcessFunction[Long, TaxiRide, TaxiFinder, TaxiRide] {
    private val rideStateDiscriptor =  new ValueStateDescriptor[TaxiRide]("taxi-ride", classOf[TaxiRide])
    private lazy val rideState = getRuntimeContext.getState(rideStateDescriptor)

    override def processElement(ride: TaxiRide, ctx: KeyedBroadcastProcessFunction[Long, TaxiRide, TaxiFinder, TaxiRide]#ReadOnlyContext, out: Collector[TaxiRide]): Unit = {
      rideState.update(ride)
    }

    override def processBroadcastElement(value: TaxiFinder, ctx: KeyedBroadcastProcessFunction[Long, TaxiRide, TaxiFinder, TaxiRide]#Context, out: Collector[TaxiRide]): Unit = {
      ctx.applyToKeyedState(rideStateDiscriptor, new KeyedStateFunction[Long, ValueState[TaxiRide]] {
        override def process() {
          val ee = new ExpressionEvaluator()
          ee.setParameters(
            Array("watermark", "ride.rideId", "ride.taxiId", "ride.driverId", "ride.isStart",
              "ride.startTime", "ride.endTime", "ride.passengerCnt",
              "ride.startLon", "ride.startlat", "ride.endLon", "ride.endLat"),
            Array(classOf[Long], classOf[Long], classOf[Long], classOf[Long], classOf[Boolean],
              classOf[String], classOf[String], classOf[Short],
              classOf[Float], classOf[Float], classOf[Float], classOf[Float]))
          ee.setExpressionType(classOf[Boolean])
          if (ee.cook(value.condition)) out.collect(ride)
        }
      })
    }
  }
}
