package com.skt.skon.taxi

import com.skt.skon.taxi.datatypes.TaxiFare
import com.skt.skon.taxi.sources.{CheckpointedTaxiFareSource, CheckpointedTaxiRideSource}
import com.skt.skon.taxi.utils.TaxiExecutionEnvironment
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object HourlyTips {
  def main(args: Array[String]): Unit = {
    val maxDelay = 60 // events are out of order by max 60 seconds
    val servingSpeedFactor = 1800   // events of 30 minutes are served in 1 second

    // set up the streaming execution environment
    val env = TaxiExecutionEnvironment.getExecutionEnvironment()

    val hourlyTipsStream = env
      .addSource[TaxiFare](new CheckpointedTaxiFareSource(TaxiExecutionEnvironment.fareSourcePath, servingSpeedFactor))
      .map(f => (f.driverId, f.tip))
      .keyBy(_._1)
      //.timeWindowAll(Time.hours(1L))
      .window(TumblingEventTimeWindows.of(Time.hours(1L)))
      .reduce((f1, f2) => (f1._1, f1._2 + f1._2),
        (key: Long, window: TimeWindow, value: Iterable[(Long, Float)], out: Collector[(Long, Long, Float)]) => {
          val sum = value.iterator.next
          out.collect((window.getEnd, sum._1, sum._2))
        })

    val hourlyMaxTipStream = hourlyTipsStream
      //.timeWindowAll(Time.hours(1L))
      .windowAll(TumblingEventTimeWindows.of(Time.hours(1L)))
      .maxBy(2)
      .print

    env.execute("Hourly Tips for Taxi drivers")
  }
}
