package pipelines

import org.apache.spark.rdd.RDD

/**
 * An Unsupervised estimator. An estimator can emit a transformer via a call to its fit method on unlabeled data.
 *
 * @tparam I Input type.
 * @tparam O Output type.
 */
abstract class UnsupervisedEstimator[I, O] extends Serializable with PipelineStage[I, O] {
  def fit(in: RDD[I]): Transformer[I, O]
}

/**
 * A Supervised estimator. A supervised estimator emits a transformer via a call to its fit method on labeled data.
 *
 * @tparam I Input type.
 * @tparam O Output type.
 * @tparam L Label type.
 */
abstract class SupervisedEstimator[I, O, L] extends Serializable with PipelineStage[I, O] {
  def fit(data: RDD[I], labels: RDD[L]): Transformer[I, O]
}

/**
 * An unsupervised estimator that carries its data with it.
 * @param e Estimator.
 * @param data Data.
 * @tparam I Input type.
 * @tparam O Output type.
 */
private case class UnsupervisedEstimatorWithData[I, O](
    e: UnsupervisedEstimator[I, O],
    data: RDD[I]) extends PipelineStage[I, O]

/**
 * A supervised estimator that carries its data with it.
 * @param e Estimator.
 * @param data Data.
 * @param labels Labels.
 * @tparam I Input type.
 * @tparam O Output type.
 * @tparam L Label type.
 */
private case class SupervisedEstimatorWithData[I, O, L](
    e: SupervisedEstimator[I, O, L],
    data: RDD[I],
    labels: RDD[L]) extends PipelineStage[I, O]