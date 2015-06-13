package workflow

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * A node representing an Estimator with the data it is to be fit on in [[OldPipeline.fit()]]
 * (after said data goes through the prefixing part of the pipeline)
 */
case class EstimatorWithData[A, B : ClassTag] private[workflow] (estimator: Estimator[A, B], data: RDD[_]) extends OldNode[A, B] {
  def rewrite: Seq[OldNode[_, _]] = estimator match {
    case PipelineWithEstimator(prefix, innerEst) => {
      prefix.rewrite ++ EstimatorWithData(innerEst, data).rewrite
    }
    case _ => Seq(this)
  }

  /**
   * @return false,
   * because the type of the data must match the input type of the enclosing [[OldPipeline]] so [[OldPipeline.fit()]]
   * works correctly
   */
  def canSafelyPrependExtraNodes: Boolean = false
}
