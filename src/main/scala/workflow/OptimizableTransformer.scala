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