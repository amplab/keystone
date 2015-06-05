package workflow

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by tomerk11 on 5/28/15.
 */
case class EstimatorWithData[A, B : ClassTag] private[workflow] (estimator: Estimator[A, B], data: RDD[_]) extends Node[A, B] {
  def rewrite: Seq[Node[_, _]] = Seq(this)

  def canSafelyPrependExtraNodes: Boolean = false
}
