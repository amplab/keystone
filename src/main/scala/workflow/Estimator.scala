package workflow

import java.io.Serializable

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait EstimatorPipeline[A, B, C, T <: ExposableTransformer[B, C, T]] {
  def fit(data: RDD[A]): SingleTransformerPipeline[B, C, T]
}

/**
 * An estimator has a `fit` method which takes an input and emits a [[Transformer]]
 * @tparam A The type of input this estimator (and the resulting Transformer) takes
 * @tparam B The output type of the node this estimator produces when being fit
 */
abstract class ModelExposingEstimator[A, B, T <: ExposableTransformer[A, B, T]] extends EstimatorNode with EstimatorPipeline[A, A, B, T]  {
  /**
   * An estimator has a `fit` method which emits a [[Transformer]].
   * @param data Input data.
   * @return A [[Transformer]] which can be called on new data.
   */
  override def fit(data: RDD[A]): T

  override def fit(dependencies: Seq[RDD[_]]): TransformerNode[_] = fit(dependencies.head.asInstanceOf[RDD[A]])
}

/**
 * An estimator has a `fit` method which takes an input and emits a [[Transformer]]
 * @tparam A The type of input this estimator (and the resulting Transformer) takes
 * @tparam B The output type of the node this estimator produces when being fit
 */
abstract class Estimator[A, B : ClassTag] extends ModelExposingEstimator[A, B, Transformer[A, B]]

object Estimator extends Serializable {
  /**
   * This constructor takes a function and returns an estimator. The function must itself return a [[Transformer]].
   *
   * @param node An estimator function. It must return a function.
   * @tparam I Input type of the estimator and the transformer it produces.
   * @tparam O Output type of the estimator and the transformer it produces.
   * @return An Estimator which can be applied to new data.
   */
  def apply[I, O : ClassTag](node: RDD[I] => Transformer[I, O]): Estimator[I, O] = new Estimator[I, O] {
    override def fit(rdd: RDD[I]): Transformer[I, O] = node.apply(rdd)
  }
}
