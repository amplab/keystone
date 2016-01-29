package workflow

import org.apache.spark.rdd.RDD

/**
 * Represents a node-level optimizable Estimator and its optimization rules
 */
abstract class OptimizableEstimator[A, B] extends Estimator[A, B] {
  val default: Estimator[A, B]

  // By default should just fit using whatever the default is.
  // Due to some crazy Scala compiler shenanigans we need to do this roundabout fitRDDs call thing.
  override def fit(data: RDD[A]): Transformer[A, B] = default.fitRDDs(Seq(data)).asInstanceOf[Transformer[A, B]]

  def optimize(sample: RDD[A], numPerPartition: Map[Int, Int]): Estimator[A, B]
}
