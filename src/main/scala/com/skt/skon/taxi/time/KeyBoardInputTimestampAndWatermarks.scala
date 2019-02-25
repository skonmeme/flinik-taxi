package com.skt.skon.taxi.time

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class KeyBoardInputTimestampAndWatermarks[T] extends AssignerWithPeriodicWatermarks[T] {
  override def extractTimestamp(element: T, previousElementTimestamp: Long): Long = 0L

  override def getCurrentWatermark: Watermark = Watermark.MAX_WATERMARK
}
