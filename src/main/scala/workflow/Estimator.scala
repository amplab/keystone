package workflow

import java.io.Serializable

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * An estimator has a `fit` method which takes an input and emits a [[Transformer]]
 * @tparam A The type of input this estimator (and the resulting Transformer) takes
 * @tparam B The output type of the node this estimator produces when being fit
 */
abstract class Estimator[A, B : ClassTag] extends EstimatorNode  {
  /**
   * An estimator has a `fit` method which emits a [[Transformer]].
   * @param data Input data.
   * @return A [[Transformer]] which can be called on new data.
   */
  protected def fit(data: RDD[A]): Transformer[A, B]

  private[workflow] final def fit(dependencies: Seq[RDD[_]]): TransformerNode[_] = fit(dependencies.head.asInstanceOf[RDD[A]])

  def withData(data: RDD[A]): Pipeline[A, B] = {
    val nodes: Seq[Node] = Seq(DataNode(data), this, new DelegatingTransformer[B](this.label + ".fit"))
    val dataDeps = Seq(Seq(), Seq(0), Seq(Pipeline.SOURCE))
    val fitDeps = Seq(Seq(), Seq(), Seq(1))
    val sink = 2

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
  def apply[I, O : ClassTag](node: RDD[I] => Transformer[I, O]): Estimator[I, O] = new Estimator[I, O] {
    override def fit(rdd: RDD[I]): Transformer[I, O] = node.apply(rdd)
  }
}
