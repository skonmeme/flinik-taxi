package com.skt.skon.taxi

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
    case class A(time: Long, key: Long, value: List[Long])
 1. timeWindows(Time.seconds(3))

    0~2999
      W(0),
      A(1, 0, List(1, 1)),
      A(2, 0, List(1, 2)), --> triggering on even timestamp at end of window (0, 2999, 6)
      W(2),
                           --> triggering on end of window (2000, 2999, 6)
    3000~5999
      A(3, 0, List(1, 3)),
      A(4, 0, List(1, 4)), --> triggering on even timestamp at end of window (2000, 5999, 14)
      W(5),
                           --> triggering on end of window (5000, 5999, 14)
    6000~8999
      A(6, 0, List(1, 5)), --> triggering on even timestamp at end of window (2000, 8999, 5)
      A(7, 0, List(1, 6)),
      W(8))
                           --> triggering on end of window (8000, 8999, 22)

    6> (0,2999,6)
    6> (2000,5999,14)
    6> (2000,2999,6)
    (5000,8999,5)on ProcessFunction
    6> (5000,5999,14)
    6> (8000,8999,22)

 2. timeWindows(Time.seconds(5))

    0~~4999
      W(0),
      A(1, 0, List(1, 1)),
      A(2, 0, List(1, 2)), --> triggering on even timestamp at end of window (0, 4999, 6)
      W(2),
      A(3, 0, List(1, 3)),
      A(4, 0, List(1, 4)), --> triggering on even timestamp at end of window (2000, 4999, 40)
                           --> triggering on end of window (2000, 8999, 40)
    5000~9999
      W(5),
      A(6, 0, List(1, 5)), --> triggering on even timestamp at end of window (5000, 9999, 5)
      A(7, 0, List(1, 6)),
      W(8))
                           --> triggering on end of window (8000, 9999, 22)

    6> (0,4999,6)
    6> (2000,4999,40)
    6> (2000,4999,40)
    (5000,9999,5)on ProcessFunction
    6> (8000,9999,22)

 3. timeWindows(Time.seconds(60))

    0~~59999
      W(0),
      A(1, 0, List(1, 1)),
      A(2, 0, List(1, 2)), --> triggering on even timestamp at end of window (0, 59999, 6)
      W(2),
      A(3, 0, List(1, 3)),
      A(4, 0, List(1, 4)), --> triggering on even timestamp at end of window (2000, 59999, 40)
      W(5),
      A(6, 0, List(1, 5)), --> triggering on even timestamp at end of window (5000, 59999, 75)
      A(7, 0, List(1, 6)),
      W(8))
                           --> triggering on end of window (8000, 59999, 126)

    6> (0,59999,6)
    6> (2000,59999,40)
    (5000,59999,75)on ProcessFunction
    6> (8000,59999,126)


Conclusion:
timestamp does not depend on 'real time'. Above results are returned on every second (when sourced from sourceFunction)

*/

object C extends LazyLogging {
  case class A(time: Long, key: Long, value: List[Long])
  case class W(time: Long)

  def main(args: Array[String]): Unit = {
    val as = Seq(
      W(0),
      A(1, 0, List(1, 1)),
      A(2, 0, List(1, 2)),
      W(2),
      A(3, 0, List(1, 3)),
      A(4, 0, List(1, 4)),
      W(5),
      A(6, 0, List(1, 5)),
      A(7, 0, List(1, 6)),
      W(8),
      W(9),
      W(10),
      W(11),
      W(12),
      W(13),
      W(14),
      W(15),
      W(16),
      W(17),
      W(18),
      W(19),
      W(20),
      W(21),
      W(22),
      W(23),
      W(24),
      W(25),
      W(26),
      W(27),
      W(28),
      W(29),
      W(30),
      W(31),
      W(32),
      W(33),
      W(34),
      W(35),
      W(36),
      W(37),
      W(38),
      W(39),
      W(40),
      W(41),
      W(42),
      A(43, 0, List(1, 7)),
      W(44),
      W(45),
      W(46),
      W(47),
      W(58),
      W(49),
      A(50, 0, List(1, 8)),
      W(51),
      W(52),
      W(53),
      W(54),
      W(55),
      W(56),
      W(57),
      A(58, 0, List(1, 9)),
      W(59))

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
      .timeWindow(Time.seconds(3))
      //.window(EventTimeSessionWindows.withGap(Time.seconds(15)))
      .trigger(new EvenTimeTrigger)
      .aggregate(new AG)
      .process(new ResultProcesssFunction)

    aStream.print

    env.execute("ProcessAfterAggregation")
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

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult =
      TriggerResult.CONTINUE

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
      ctx.deleteEventTimeTimer(window.maxTimestamp)
    }

    override def canMerge: Boolean = true

    override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit =
      if (window.maxTimestamp > ctx.getCurrentWatermark) ctx.registerEventTimeTimer(window.maxTimestamp)

    override def toString = "EvenTimeTrigger()"
  }

  class AG extends AggregateFunction[A, (Long, Long), List[Long]] {
    override def createAccumulator(): (Long, Long) = (0L, 0L)

    override def add(value: A, accumulator: (Long, Long)): (Long, Long) = (accumulator._1 + value.value.head, accumulator._2 + value.value(1))

    override def getResult(accumulator: (Long, Long)): List[Long] = List(accumulator._1, accumulator._2)

    override def merge(a: (Long, Long), b: (Long, Long)): (Long, Long) = (a._1 + b._1, a._2 + b._2)
  }

  /*
  class WP extends ProcessWindowFunction[Long, (Long, Long), Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[(Long, Long)]): Unit = {
      val sum = elements.iterator.next
      if (context.window.getEnd <= context.currentWatermark)
        Console.println(String.valueOf((context.currentWatermark, sum)) + " from ProcessAllWindowFunction")
      else
        out.collect((context.currentWatermark, sum))
    }
  }
  */

  class ResultProcesssFunction extends ProcessFunction[List[Long], (Long, Long, String)] {
    override def processElement(value: List[Long], ctx: ProcessFunction[List[Long], (Long, Long, String)]#Context, out: Collector[(Long, Long, String)]): Unit = {
      out.collect((ctx.timerService.currentWatermark, ctx.timestamp, value.toString))
    }
  }
}
