package com.skt.skon.taxi.utils

import com.skt.skon.taxi.utils.TaxiExecutionMode._
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

case class ExecutionEnvironment(env: StreamExecutionEnvironment, mode: Mode) {
  def addSource[T](function: SourceFunction[T]): DataStream[T] = {
    mode match {
      case Normal => env.addSource(function)
      case Test => env.addSource(function)
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
    ExecutionEnvironment(StreamExecutionEnvironment.getExecutionEnvironment, executionMode)
  }

  def print[T](out: DataStream[T]): DataStreamSink[T] = {
    executionMode match {
      case Normal => out.print
      case Test => out.print
    }
  }
}
