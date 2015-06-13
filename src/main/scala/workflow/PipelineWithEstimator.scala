package workflow

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by tomerk11 on 5/28/15.
 */
private[workflow] case class PipelineWithEstimator[A, B : ClassTag, C : ClassTag](pipeline: OldPipeline[A, B], estimator: Estimator[B, C]) extends Estimator[A, C] {
  override def withData(data: RDD[A]) = {
    pipeline then EstimatorWithData(estimator, data)
  }

  def fit(data: RDD[A]): Transformer[A, C] = withData(data).fit()
}
