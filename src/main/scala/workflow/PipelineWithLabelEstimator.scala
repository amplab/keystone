package workflow

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by tomerk11 on 5/28/15.
 */
private[workflow] case class PipelineWithLabelEstimator[A, B : ClassTag, C : ClassTag, L] (pipeline: OldPipeline[A, B], estimator: LabelEstimator[B, C, L]) extends LabelEstimator[A, C, L] {
  override def withData(data: RDD[A], labels: RDD[L]) = {
    pipeline then LabelEstimatorWithData(estimator, data, labels)
  }

  def fit(data: RDD[A], labels: RDD[L]): Transformer[A, C] = withData(data, labels).fit()
}
