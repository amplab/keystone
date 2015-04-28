package pipelines

import java.io.Serializable

/**
 * An estimator has a `fit` method which takes an input and emits a pipeline node
 * @tparam A The type of input this estimator (and the resulting pipeline node) takes
 * @tparam B The output type of the node this estimator produces when being fit
 */
abstract class Estimator[-A, +B] extends Serializable {
  /**
   * An estimator has a `fit` method which emits a pipeline node.
   * @param data Input data.
   * @return A PipelineNode (Transformer) which can be called on new data.
   */
  def fit(data: A): PipelineNode[A, B]
}

object Estimator extends Serializable {
  /**
   * This constructor takes a function and returns an estimator. The function must itself return a pipeline node.
   *
   * @param node An estimator function. It must return a function.
   * @tparam I Input type of the estimator and the transformer it produces.
   * @tparam O Output type of the estimator and the transformer it produces.
   * @return An Estimator which can be applied to new data.
   */
  def apply[I, O](node: I => PipelineNode[I, O]): Estimator[I, O] = new Estimator[I, O] {
    override def fit(v1: I): PipelineNode[I, O] = node.apply(v1)
  }
}
