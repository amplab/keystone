package workflow

import java.io.Serializable

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait LabelEstimatorPipeline[A, B, C, T <: ExposableTransformer[B, C, T], L] {
  def fit(data: RDD[A], labels: RDD[L]): SingleTransformerPipeline[B, C, T]
}

abstract class ModelExposingLabelEstimator[A, B, T <: ExposableTransformer[A, B, T], L] extends EstimatorNode with LabelEstimatorPipeline[A, A, B, T, L]  {
  /**
   * A LabelEstimator estimator is an estimator which expects labeled data.
   * @param data Input data.
   * @param labels Input labels.
   * @return A [[Transformer]] which can be called on new data.
   */
  override def fit(data: RDD[A], labels: RDD[L]): T

  override def fit(dependencies: Seq[RDD[_]]): TransformerNode[_] = fit(dependencies(0).asInstanceOf[RDD[A]], dependencies(1).asInstanceOf[RDD[L]])
}

/**
 * A label estimator has a `fit` method which takes input data & labels and emits a [[Transformer]]
 * @tparam I The type of the input data
 * @tparam O The type of output of the emitted transformer
 * @tparam L The type of label this node expects
 */
abstract class LabelEstimator[I, O : ClassTag, L] extends ModelExposingLabelEstimator[I, O, Transformer[I, O], L]

object LabelEstimator extends Serializable {
  /**
   * This constructor takes a function which expects labeled data and returns an estimator.
   * The function must itself return a transformer.
   *
   * @param node An estimator function. It must take labels and data and return a function from data to output.
   * @tparam I Input type of the labeled estimator and the transformer it produces.
   * @tparam O Output type of the estimator and the transformer it produces.
   * @tparam L Label type of the estimator.
   * @return An Estimator which can be applied to new labeled data.
   */
  def apply[I, O : ClassTag, L](node: (RDD[I], RDD[L]) => Transformer[I, O]): LabelEstimator[I, O, L] = new LabelEstimator[I, O, L] {
    override def fit(v1: RDD[I], v2: RDD[L]): Transformer[I, O] = node(v1, v2)
  }
}
