package pipelines

import java.io.Serializable

/**
 * A label estimator has a `fit` method which takes input data & labels and emits a pipeline node
 * @tparam I The type of the input data
 * @tparam O The type of output of the emitted pipeline node
 * @tparam L The type of label this node expects
 */
abstract class LabelEstimator[-I, +O, -L] extends Serializable {
  /**
   * A LabelEstimator estimator is an estimator which expects labeled data.
   * @param data Input data.
   * @param labels Input labels.
   * @return A PipelineNode which can be called on new data.
   */
  def fit(data: I, labels: L): PipelineNode[I, O]
}

object LabelEstimator extends Serializable {
  /**
   * This constructor takes a function which expects labeled data and returns an estimator.
   * The function must itself return a pipeline node.
   *
   * @param node An estimator function. It must take labels and data and return a function from data to output.
   * @tparam I Input type of the labeled estimator and the transformer it produces.
   * @tparam O Output type of the estimator and the transformer it produces.
   * @tparam L Label type of the estimator.
   * @return An Estimator which can be applied to new labeled data.
   */
  def apply[I, O, L](node: (I, L) => PipelineNode[I, O]): LabelEstimator[I, O, L] = new LabelEstimator[I, O, L] {
    override def fit(v1: I, v2: L): PipelineNode[I, O] = node(v1, v2)
  }
}
