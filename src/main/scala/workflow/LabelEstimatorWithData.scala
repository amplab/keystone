package workflow

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * A node representing a LabelEstimator with the data it is to be fit on in [[Pipeline.fit()]]
 * (after said data goes through the prefixing part of the pipeline)
 */
case class LabelEstimatorWithData[A, B : ClassTag, L] private[workflow] (estimator: LabelEstimator[A, B, L], data: RDD[_], labels: RDD[_]) extends Node[A, B] {
  def rewrite: Seq[Node[_, _]] = estimator match {
    case PipelineWithLabelEstimator(prefix, innerEst) => {
      prefix.rewrite ++ LabelEstimatorWithData(innerEst, data, labels).rewrite
    }
    case _ => Seq(this)
  }

  /**
   * @return false,
   * because the type of the data must match the input type of the enclosing [[Pipeline]] so [[Pipeline.fit()]]
   * works correctly
   */
  def canSafelyPrependExtraNodes: Boolean = false
}
