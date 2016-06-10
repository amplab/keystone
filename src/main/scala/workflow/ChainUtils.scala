package workflow

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * A chain of two Transformers in a row (as a Transformer)
 * @param first
 * @param second
 */
case class TransformerChain[A, B, C : ClassTag](first: Transformer[A, B], second: Transformer[B, C]) extends Transformer[A, C] {
  override def apply(in: A): C = second(first(in))
  override def apply(in: RDD[A]): RDD[C] = second(first(in))
}

/**
 * A chain of a Transformer followed by an Estimator (as an Estimator)
 * @param first
 * @param second
 */
case class TransformerEstimatorChain[A, B, C : ClassTag](first: Transformer[A, B], second: Estimator[B, C])
  extends Estimator[A, C] {

  override def fit(data: RDD[A]): Transformer[A, C] = {
    TransformerChain(first, second.fit(first(data)))
  }
}

/**
 * A chain of a Transformer followed by a LabelEstimator (as a LabelEstimator)
 * @param first
 * @param second
 */
case class TransformerLabelEstimatorChain[A, B, C : ClassTag, L](first: Transformer[A, B], second: LabelEstimator[B, C, L])
  extends LabelEstimator[A, C, L] {

  override def fit(data: RDD[A], labels: RDD[L]): Transformer[A, C] = {
    TransformerChain(first, second.fit(first(data), labels))
  }
}
