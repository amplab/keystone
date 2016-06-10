package workflow

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

sealed trait Optimizable

/**
 * Represents a node-level optimizable transformer and its optimization rules
 */
abstract class OptimizableTransformer[A, B : ClassTag] extends Transformer[A, B] with Optimizable {
  val default: Transformer[A, B]
  override def apply(a: A): B = {
    default.apply(a)
  }
  override def apply(data: RDD[A]): RDD[B] = {
    default.apply(data)
  }

  def optimize(sample: RDD[A], numPerPartition: Map[Int, Int]): Transformer[A, B]
}

/**
 * Represents a node-level optimizable Estimator and its optimization rules
 */
abstract class OptimizableEstimator[A, B] extends Estimator[A, B] with Optimizable {
  val default: Estimator[A, B]

  // Fit using whatever the default is.
  override def fit(data: RDD[A]): Transformer[A, B] = {
    default.fit(data)
  }

  def optimize(sample: RDD[A], numPerPartition: Map[Int, Int]): Estimator[A, B]
}

/**
 * Represents a node-level optimizable LabelEstimator and its optimization rules
 */
abstract class OptimizableLabelEstimator[A, B, L] extends LabelEstimator[A, B, L] with Optimizable {
  val default: LabelEstimator[A, B, L]

  // Fit using whatever the default is.
  override def fit(data: RDD[A], labels: RDD[L]): Transformer[A, B] = {
    default.fit(data, labels)
  }

  def optimize(sample: RDD[A], sampleLabels: RDD[L], numPerPartition: Map[Int, Int]): LabelEstimator[A, B, L]
}
