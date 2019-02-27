package com.skt.skon.taxi

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
    case class A(time: Long, key: Long, value: Long)
 1. timeWindows(Time.seconds(3))

    0~~2999
          W(0L),
          A(1L, 0, 1L),
          A(1L, 0, 2L),
          W(1L),
          A(2L, 0, 2L), --> trigger with even time
    3000~5999           --> end of window
          A(3L, 0, 2L),
          W(3L),
          A(4L, 0, 2L), --> tirrger with even time
          A(5L, 0, 2L),
          W(5L))
    6000~               --> end of window

    6> (1000,5)
    (3000,5) from ProcessAllWindowFunction
    6> (3000,4)
    (9223372036854775807,6) from ProcessAllWindowFunction


 2. timeWindows(Time.seconds(5))

    0~~4999
          W(0L),
          A(1L, 0, 1L),
          A(1L, 0, 2L),
          W(1L),
          A(2L, 0, 2L), --> trigger with even time
          A(3L, 0, 2L),
          W(3L),
          A(4L, 0, 2L), --> tirrger with even time
    5000~9999           --> end of window
          A(5L, 0, 2L),
          W(5L))
    10000~              --> end of window

    6> (1000,5)
    6> (3000,9)
    (5000,9) from ProcessAllWindowFunction
    (9223372036854775807,2) from ProcessAllWindowFunction
*/

object B extends LazyLogging {
  case class A(time: Long, key: Long, value: Long)
  case class W(time: Long)

  def main(args: Array[String]): Unit = {
    val as = Seq(
      W(0),
      A(1, 0, 1),
      A(1, 0, 2),
      W(1),
      A(2, 0, 2),
      A(3, 0, 2),
      W(3),
      A(4, 0, 2),
      A(5, 0, 2),
      W(5))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val aStream = env
      .addSource(new SourceFunction[A] {
        override def run(ctx: SourceFunction.SourceContext[A]): Unit = {
          val ai = as.iterator
          while (ai.hasNext) {
            ai.next match {
              case v@A(time, _, _) => ctx.collectWithTimestamp(v, time * 1000L)
              case W(time) => ctx.emitWatermark(new Watermark(time * 1000L))
            }
            Thread.sleep(1000L)
          }
        }

        override def cancel(): Unit = {}
      })
      .keyBy(_.key)
//      .timeWindow(Time.seconds(5))
      .timeWindow(Time.seconds(3))
      .trigger(new EvenTimeTrigger)
      .aggregate(new AG, new WP)

    aStream.print

    env.execute("ProcessWindowFunction")
  }

  class EvenTimeTrigger extends Trigger[A, TimeWindow] {
    override def onElement(element: A, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      if (window.maxTimestamp <= ctx.getCurrentWatermark || element.time % 2 == 0) {
        TriggerResult.FIRE
      } else {
        ctx.registerEventTimeTimer(window.maxTimestamp)
        TriggerResult.CONTINUE
      }
    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      if (time == window.maxTimestamp) TriggerResult.FIRE
      else TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
      ctx.deleteEventTimeTimer(window.maxTimestamp)
    }

    override def canMerge: Boolean = true

    override def toString = "EvenTimeTrigger()"
  }

  class AG extends AggregateFunction[A, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: A, accumulator: Long): Long = accumulator + value.value

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  class WP extends ProcessWindowFunction[Long, (Long, Long), Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[(Long, Long)]): Unit = {
      val sum = elements.iterator.next
      if (context.window.getEnd <= context.currentWatermark)
        Console.println(String.valueOf((context.currentWatermark, sum)) + " from ProcessAllWindowFunction")
      else
        out.collect((context.currentWatermark, sum))
    }
  }
}
