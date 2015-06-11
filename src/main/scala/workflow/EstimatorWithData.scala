package workflow

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * A node representing an Estimator with the data it is to be fit on in [[Pipeline.fit()]]
 * (after said data goes through the prefixing part of the pipeline)
 */
case class EstimatorWithData[A, B : ClassTag] private[workflow] (estimator: Estimator[A, B], data: RDD[_]) extends Node[A, B] {
  def rewrite: Seq[Node[_, _]] = estimator match {
    case PipelineWithEstimator(prefix, innerEst) => {
      prefix.rewrite ++ EstimatorWithData(innerEst, data).rewrite
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
