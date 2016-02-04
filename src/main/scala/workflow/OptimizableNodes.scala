package workflow

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Represents a node-level optimizable transformer and its optimization rules
 */
abstract class OptimizableTransformer[A, B : ClassTag] extends Transformer[A, B] {
  val default: Transformer[A, B]
  override def apply(a: A) = default.apply(a)
  override def apply(data: RDD[A]) = default.apply(data)

  def optimize(sample: RDD[A], numPerPartition: Map[Int, Int]): Transformer[A, B]
}

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
