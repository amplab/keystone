package org.apache.spark.util

object SparkUtilWrapper {
  def estimateSize(obj: AnyRef): Long = SizeEstimator.estimate(obj)
}