package workflow

import org.apache.spark.rdd.RDD

/**
 * Represents a node-level optimizable LabelEstimator and its optimization rules
 */
abstract class OptimizableLabelEstimator[A, B, L] extends LabelEstimator[A, B, L] {
  val default: LabelEstimator[A, B, L]

  // By default should just fit using whatever the default is.
  // Due to some crazy Scala compiler shenanigans we need to do this roundabout fitRDDs call thing.
  override protected def fit(data: RDD[A], labels: RDD[L]): Transformer[A, B] = {
    default.fitRDDs(Seq(data, labels)).asInstanceOf[Transformer[A, B]]
  }

  def optimize(sample: RDD[A], sampleLabels: RDD[L], numPerPartition: Map[Int, Int]): LabelEstimator[A, B, L]
}
