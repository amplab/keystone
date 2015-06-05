package workflow

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by tomerk11 on 5/28/15.
 */
case class LabelEstimatorWithData[A, B : ClassTag, L] private[workflow] (estimator: LabelEstimator[A, B, L], data: RDD[_], labels: RDD[_]) extends Node[A, B] {
  def rewrite: Seq[Node[_, _]] = Seq(this)

  def canSafelyPrependExtraNodes: Boolean = false
}
