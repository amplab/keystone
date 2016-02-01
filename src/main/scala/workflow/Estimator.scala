package workflow

import java.io.Serializable

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * An estimator has a `fit` method which takes an input and emits a [[Transformer]]
 * @tparam A The type of input this estimator (and the resulting Transformer) takes
 * @tparam B The output type of the node this estimator produces when being fit
 */
abstract class Estimator[A, B] extends EstimatorNode  {
  /**
   * An estimator has a `fit` method which emits a [[Transformer]].
   * @param data Input data.
   * @return A [[Transformer]] which can be called on new data.
   */
  protected def fit(data: RDD[A]): Transformer[A, B]

  private[workflow] final def fit(dependencies: Seq[RDD[_]]): TransformerNode = fit(dependencies.head.asInstanceOf[RDD[A]])

  /**
   * Constructs a pipeline from a single estimator and training data.
   * Equivalent to `Pipeline() andThen (estimator, data)`
   *
   * @param data The training data
   */
  def withData(data: RDD[A]): Pipeline[A, B] = {
    val nodes: Seq[Node] = Seq(SourceNode(data), this, new DelegatingTransformerNode(this.label + ".fit"))
    val dataDeps = Seq(Seq(), Seq(0), Seq(Pipeline.SOURCE))
    val fitDeps = Seq(None, None, Some(1))
    val sink = nodes.size - 1

    Pipeline[A, B](nodes, dataDeps, fitDeps, sink)
  }
}

object Estimator extends Serializable {
  /**
   * This constructor takes a function and returns an estimator. The function must itself return a [[Transformer]].
   *
   * @param node An estimator function. It must return a function.
   * @tparam I Input type of the estimator and the transformer it produces.
   * @tparam O Output type of the estimator and the transformer it produces.
   * @return An Estimator which can be applied to new data.
   */
  def apply[I, O](node: RDD[I] => Transformer[I, O]): Estimator[I, O] = new Estimator[I, O] {
    override def fit(rdd: RDD[I]): Transformer[I, O] = node.apply(rdd)
  }
}
