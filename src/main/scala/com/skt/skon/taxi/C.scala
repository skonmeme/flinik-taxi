package com.skt.skon.taxi

import java.util.Calendar

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.api.common.state.{ListStateDescriptor, ReducingStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/*
case 1: When there are watermarks among 5->4 (usual case),
      W(0L),
      A(1000L, 0, 1),
      A(2000L, 0, 4),
      A(3000L, 0, 4),
      A(4000L, 0, 4),
      W(5000L),
      A(6000L, 0, 5),
      W(6500L),
      A(7000L, 0, 4),
      W(10000L),
      W(20000L), ...)

  result:
    (fired at,6500)
    6> (11000,6500,List(1, 4, 4, 4, 5))                   ---> W(6500)
    (fired at,20000)
    (12000,20000,List(1, 4, 4, 4, 4, 5)) at Window End    ---> W(20000)

case 2: When there is no watermarks among 5->4 (rare case),
      W(0L),
      A(1000L, 0, 1),
      A(2000L, 0, 4),
      A(3000L, 0, 4),
      A(4000L, 0, 4),
      W(5000L),
      A(6000L, 0, 5),
      //W(6500L),
      A(7000L, 0, 4),
      W(10000L),
      W(20000L), ...)

  result:
    (fired at,10000)
    6> (12000,10000,List(1, 4, 4, 4, 4, 5))               ---> W(10000)
    (continued at,20000)                                  ---> W(20000)
 */
object C extends LazyLogging {
  case class A(timestamp: Long, key: Long, value: Long)
  case class W(timestamp: Long)

  def main(args: Array[String]): Unit = {
    val as = Seq(
      W(0L),
      A(1000L, 0, 1),
      A(2000L, 0, 4),
      A(3000L, 0, 4),
      A(4000L, 0, 4),
      W(5000L),
      A(6000L, 0, 5),
      //W(6500L),
      A(7000L, 0, 4),
      W(10000L),
      W(20000L),
      W(30000L),
      W(40000L),
      W(50000L),
      W(60000L),
      W(70000L),
      W(80000L))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val aStream = env
      .addSource(new SourceFunction[A] {
        override def run(ctx: SourceFunction.SourceContext[A]): Unit = {
          val ai = as.iterator
          while (ai.hasNext) {
            ai.next match {
              case v@A(timestamp, _, _) => ctx.collectWithTimestamp(v, timestamp)
              case W(timestamp) => ctx.emitWatermark(new Watermark(timestamp))
            }
          }
        }

        override def cancel(): Unit = {}
      })
      .keyBy(_.key)
      //      .timeWindow(Time.seconds(5))
      .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
      .trigger(new EarlyResultEventTimeTrigger(_.value == 5))
      .aggregate(new AG, new WP)
      .print

    env.execute("EarlyResultEventTimeTrigger")
  }

  class EarlyResultEventTimeTrigger[T](eval: (T => Boolean)) extends Trigger[T, TimeWindow] {
    val timersDesc = new ListStateDescriptor[Long]("timers", classOf[Long])
    val countDesc = new ReducingStateDescriptor(
      "count",
      new ReduceFunction[Long]() {override def reduce(v1: Long, v2: Long): Long = v1+v2},
      classOf[Long]
    )
    val lastCountWhenFiringDesc = new ReducingStateDescriptor(
      "lastCount",
      new ReduceFunction[Long]() {override def reduce(v1: Long, v2: Long): Long = v1+v2},
      classOf[Long]
    )

    override def onElement(element: T, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      ctx.getPartitionedState(countDesc).add(1)

      if (window.maxTimestamp <= ctx.getCurrentWatermark) {
        fireOrContinue(ctx)
      } else {
        if (eval(element)) {
          ctx.registerEventTimeTimer(timestamp)
          ctx.getPartitionedState(timersDesc).add(timestamp)
        }
        ctx.registerEventTimeTimer(window.maxTimestamp)
        TriggerResult.CONTINUE
      }
    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      if (time < window.maxTimestamp) {
        ctx.deleteEventTimeTimer(time)

        val timers = ctx.getPartitionedState(timersDesc)
        timers.update(timers.get.asScala.filter(_ != time).toSeq.asJava)

        fireOrContinue(ctx)
      } else if (time == window.maxTimestamp) {
        fireOrContinue(ctx)
      } else {
        TriggerResult.CONTINUE
      }
    }

    override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
      ctx.mergePartitionedState(timersDesc)
      ctx.mergePartitionedState(countDesc)
      ctx.mergePartitionedState(lastCountWhenFiringDesc)

      val timers = ctx.getPartitionedState(timersDesc)
      if (timers.get != null) {
        timers.get.asScala.foreach(ctx.registerEventTimeTimer)
      }

      ctx.registerEventTimeTimer(window.maxTimestamp)
    }

    def fireOrContinue(ctx: Trigger.TriggerContext): TriggerResult = {
      val count_val = ctx.getPartitionedState(countDesc).get
      val lastCount = ctx.getPartitionedState(lastCountWhenFiringDesc)
      val lastCount_val = lastCount.get
      val diff = count_val - lastCount_val
      lastCount.add(diff)

      if (diff > 0) {
        Console.println("fired at", ctx.getCurrentWatermark)
        TriggerResult.FIRE
      } else {
        Console.println("continued at", ctx.getCurrentWatermark)
        TriggerResult.CONTINUE
      }
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
      ctx.deleteEventTimeTimer(window.maxTimestamp)

      ctx.getPartitionedState(timersDesc).clear()
      ctx.getPartitionedState(countDesc).clear()
      ctx.getPartitionedState(lastCountWhenFiringDesc).clear()
    }

    override def canMerge: Boolean = true

    override def toString = "EarlyResultEventTimeTrigger()"
  }

  class AG extends AggregateFunction[A, Array[Long], List[Long]] {
    override def createAccumulator(): Array[Long] = Array()

    override def add(value: A, accumulator: Array[Long]): Array[Long] = {
      val s = Calendar.getInstance().getTimeInMillis
      if (accumulator.contains(5)) {
        val i = accumulator.indexOf(5)
        val (h, t) = accumulator.splitAt(i)
        Thread.sleep(2000L)
        Console.println(Calendar.getInstance().getTimeInMillis - s + " computing latency")
        h ++ Array(value.value) ++ t
      } else {
        Thread.sleep(300L)
        Console.println(Calendar.getInstance().getTimeInMillis - s + " computing latency")
        accumulator :+ value.value
      }
    }

    override def getResult(accumulator: Array[Long]): List[Long] = accumulator.toList

    override def merge(a: Array[Long], b: Array[Long]): Array[Long] = a ++ b
  }

  class WP extends ProcessWindowFunction[List[Long], (Long, Long, List[Long]), Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[List[Long]], out: Collector[(Long, Long, List[Long])]): Unit = {
      val sum = elements.iterator.next
      if (context.window.getEnd <= context.currentWatermark)
        // at window end
        Console.println(String.valueOf((context.window.getEnd, context.currentWatermark, sum)) + " at Window End" )
      else
        // at eventCode.Finish
        out.collect((context.window.getEnd, context.currentWatermark, sum))
    }
  }
}
