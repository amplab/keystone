package workflow

import org.apache.spark.rdd.RDD

/**
 * Created by tomerk11 on 5/28/15.
 */
case class LabelEstimatorWithData[A, B, L] private[workflow] (estimator: LabelEstimator[A, B, L], data: RDD[_], labels: RDD[_]) extends Node[A, B] {
  def fit(): Transformer[A, B] = estimator.unsafeFit(data, labels)

  def rewrite: Seq[Node[_, _]] = Seq(this)

  def canElevate: Boolean = false
}
