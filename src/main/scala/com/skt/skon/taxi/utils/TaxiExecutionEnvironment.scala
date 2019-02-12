package com.skt.skon.taxi.utils

import com.skt.skon.taxi.utils.TaxiExecutionMode._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic

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
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    ExecutionEnvironment(env, executionMode)
  }

  def print[T](out: DataStream[T]): DataStreamSink[T] = {
    executionMode match {
      case Normal => out.print
      case Test => out.print
    }
  }
}
