package com.skt.skon.taxi

import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.KeyedStateFunction
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

object A {
  case class A(time: Long, value: Long)
  case class B(time: Long, key: Long, value: Long)
  case class W(time: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val as = Seq(
      A(0L, 1L),
      W(1L),
      A(2L, 2L))
    val bs = Seq(
      B(1L, 0L, 3L),
      B(3L, 1L, 4L),
      W(3L)
    )

    val broadcastStream = env
      .addSource(new SourceFunction[A] {
        override def run(ctx: SourceFunction.SourceContext[A]): Unit = {
          val ai = as.iterator
          while (ai.hasNext) {
            ai.next match {
              case v@A(time, _) => ctx.collectWithTimestamp(v, time)
              case W(time) => ctx.emitWatermark(new Watermark(time))
            }
            Thread.sleep(1000L)
          }
        }

        override def cancel(): Unit = {}
      })
      .broadcast(new MapStateDescriptor[Long, Long]("dummy", classOf[Long], classOf[Long]))

    val keyedStream = env
      .addSource(new SourceFunction[B] {
        override def run(ctx: SourceFunction.SourceContext[B]): Unit = {
          val bi = bs.iterator
          while (bi.hasNext) {
            bi.next match {
              case v@B(time, _, _) => ctx.collectWithTimestamp(v, time)
              case W(time) => ctx.emitWatermark(new Watermark(time))
            }
            Thread.sleep(1000L)
          }
        }

        override def cancel(): Unit = {}
      })
      .keyBy(_.key)

    keyedStream
      .connect(broadcastStream)
      .process(new BC)
      .print

    env.execute("Broadcast")
  }

  class BC extends KeyedBroadcastProcessFunction[Long, B, A, Long] {
    private val bd = new ValueStateDescriptor[B]("b", classOf[B])
    lazy val b: ValueState[B] = getRuntimeContext.getState(bd)

    override def processElement(value: B, ctx: KeyedBroadcastProcessFunction[Long, B, A, Long]#ReadOnlyContext, out: Collector[Long]): Unit = {
      val v = b.value

      if (v == null) b.update(value)
      if (v != null) b.clear()
    }

    override def processBroadcastElement(value: A, ctx: KeyedBroadcastProcessFunction[Long, B, A, Long]#Context, out: Collector[Long]): Unit = {
      ctx.applyToKeyedState(bd, new KeyedStateFunction[Long, ValueState[B]] {
        override def process(key: Long, state: ValueState[B]): Unit = {
          out.collect(value.value)
          Console.println(state.value)
        }
      })
    }
  }
}
