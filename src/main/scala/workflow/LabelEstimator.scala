package workflow

import java.io.Serializable

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * A label estimator has a `fit` method which takes input data & labels and emits a [[Transformer]]
 * @tparam A The type of the input data
 * @tparam B The type of output of the emitted transformer
 * @tparam L The type of label this node expects
 */
abstract class LabelEstimator[A, B : ClassTag, L] extends EstimatorNode {
  def withData(data: RDD[A], labels: RDD[L]): Pipeline[A, B] = {
    val nodes: Seq[Node] = Seq(DataNode(data), DataNode(labels), this, new DelegatingTransformer[B](this.label + ".fit"))
    val dataDeps = Seq(Seq(), Seq(), Seq(0, 1), Seq(Pipeline.SOURCE))
    val fitDeps = Seq(Seq(), Seq(), Seq(), Seq(2))
    val sink = 3

    Pipeline[A, B](nodes, dataDeps, fitDeps, sink)
  }

  /**
   * A LabelEstimator estimator is an estimator which expects labeled data.
   * @param data Input data.
   * @param labels Input labels.
   * @return A [[Transformer]] which can be called on new data.
   */
  protected def fit(data: RDD[A], labels: RDD[L]): Transformer[A, B]

  private[workflow] final def fit(dependencies: Seq[RDD[_]]): TransformerNode[_] = fit(dependencies(0).asInstanceOf[RDD[A]], dependencies(1).asInstanceOf[RDD[L]])
}

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
