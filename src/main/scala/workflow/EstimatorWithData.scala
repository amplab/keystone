package workflow

import org.apache.spark.rdd.RDD

/**
 * Created by tomerk11 on 5/28/15.
 */
case class EstimatorWithData[A, B] private[workflow] (estimator: Estimator[A, B], data: RDD[_]) extends Node[A, B] {
  def fit(): Transformer[A, B] = estimator.unsafeFit(data)

  def rewrite: Seq[Node[_, _]] = Seq(this)

  def canElevate: Boolean = false
}
