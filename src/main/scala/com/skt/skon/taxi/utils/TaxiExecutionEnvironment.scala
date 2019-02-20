package com.skt.skon.taxi.utils

import java.util.Properties

import com.skt.skon.taxi.utils.TaxiExecutionMode._
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

case class ExecutionEnvironment(env: StreamExecutionEnvironment, mode: Mode) {
  def addSource[T: TypeInformation](function: SourceFunction[T]): DataStream[T] = {
    mode match {
      case Normal => env.addSource[T](function)
      case Test => env.addSource[T](function)
    }
  }

  def execute(jobString: String): Unit = env.execute(jobString)
}

object TaxiExecutionEnvironment extends Enumeration {
  final val rideSourcePath: String = "/Users/skon/Documents/src/skonmeme/da/nycTaxiRides.gz"
  final val fareSourcePath: String = "/Users/skon/Documents/src/skonmeme/da/nycTaxiFares.gz"
  var executionMode: Mode = Normal

  def getExecutionEnvironment(mode: Mode = Normal): ExecutionEnvironment = {
    executionMode = mode
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // Event time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // checkpointing per every 10s with Exactly once mode
    env.enableCheckpointing(30 * 1000L, CheckpointingMode.EXACTLY_ONCE)
    // make sure 5s of progress happen between checkpoints
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(15 * 1000L)
    // checkpoints have to complete within one minute, or are discarded
    env.getCheckpointConfig.setCheckpointTimeout(60 * 1000L)
    // prevent the tasks from failing if an error happens in their checkpointing, the checkpoint will just be declined.
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    // allow only one checkpoint to be in progress at the same time
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    // State Backend
    env.setStateBackend(new FsStateBackend("file:///tmp/flink/checkpoints", false))

    ExecutionEnvironment(env, executionMode)
  }

  def print[T](out: DataStream[T], topic: String = "", schema: SerializationSchema[T], properties: Properties = new Properties()): DataStreamSink[T] = {
    executionMode match {
      case Normal =>
        out.addSink({
          val producer = new FlinkKafkaProducer[T](topic, schema, properties)
          producer.setWriteTimestampToKafka(true)
          producer
        })
      case Test => out.print
    }
  }
}
