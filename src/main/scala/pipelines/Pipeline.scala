package pipelines

import java.io.Serializable

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object Pipelines {

  /**
   * 3 main classes:
   * PipelineNode[A, B] extends A => B
   * Estimator[A, B] implements fit(A): PipelineNode[A, B]
   * LabelEstimator[A, B, L] implements fit(A, L): PipelineNode[A, B]

   * PipelineNode supports any input and output types, like in the original interface.

   * Although nodes & estimators are appropriately contravariant/covariant, RDDs aren't.
   * If we want that we have to wrap RDD and then have the nodes take the wrapped type as input / output

   * There is no "single-item" transform. Similar to above, if we want this we need to make a type that unifies an RDD
   * and a single item, and have the nodes take that type as input/output

   * @tparam A
   * @tparam B
   */
  abstract class PipelineNode[-A, +B] extends (A => B) with Serializable {
    /**
     * Chains an estimator onto this pipeline node, producing a new estimator that when fit on same input type as
     * this node, chains this node with the node output by the original estimator.
     * @param est The estimator to chain onto the pipeline node
     * @return  The output estimator
     */
    def thenEstimator[C : ClassTag](est: Estimator[B, C]): Estimator[A, C] = functionToEstimator((a: A) => this then est.fit(this(a)))

    /**
     * Chains a Label Estimator onto this pipeline node, producing a new Label Estimator that when fit on same input
     * type as this node, chains this node with the node output by the original Label Estimator.
     * @param est The label estimator to chain onto the pipeline node
     * @return  The output label estimator
     */
    def thenLabelEstimator[C : ClassTag, L : ClassTag](est: LabelEstimator[B, C, L]): LabelEstimator[A, C, L] = functionToLabelEstimator((a: A, l: L) => this then est.fit(this(a), l))
    def then[C : ClassTag](next: PipelineNode[B, C]): PipelineNode[A, C] = andThen(next)
  }

  abstract class Estimator[-A, +B] extends Serializable {
    /**
     * An estimator has a `fit` method which emits a pipeline node.
     * @param data Input data.
     * @return A PipelineNode (Transformer) which can be called on new data.
     */
    def fit(data: A): PipelineNode[A, B]
  }

  abstract class LabelEstimator[-I, +O, -L] extends Serializable {
    /**
     * A LabelEstimator estimator is an estimator which expects labeled data.
     * @param data Input data.
     * @param labels Input labels.
     * @return A PipelineNode which can be called on new data.
     */
    def fit(data: I, labels: L): PipelineNode[I, O]
  }

  /**
   * This implicit turns regular functions into PipelineNodes.
   * @param node A function.
   * @return A PipelineNode encapsulating the function.
   */
  implicit def functionToNode[I, O](node: I => O): PipelineNode[I, O] = new PipelineNode[I, O] {
    override def apply(v1: I): O = node(v1)
  }

  /**
   * This implicit takes a function and returns an estimator. The function must itself return a function.
   *
   * @param node An estimator function. It must return a function.
   * @tparam I Input type of the estimator and the transformer it produces.
   * @tparam O Output type of the estimator and the transformer it produces.
   * @return An Estimator which can be applied to new data.
   */
  private def functionToEstimator[I, O](node: I => (I => O)): Estimator[I, O] = new Estimator[I, O] {
    override def fit(v1: I): PipelineNode[I, O] = node.apply(v1)
  }

  /**
   * This implicit takes a function which expects labeled data and returns an estimator.
   * The function must itself return a function.
   *
   * @param node An estimator function. It must take labels and data and return a function from data to output.
   * @tparam I Input type of the labeled estimator and the transformer it produces.
   * @tparam O Output type of the estimator and the transformer it produces.
   * @tparam L Label type of the estimator.
   * @return An Estimator which can be applied to new labeled data.
   */
  private def functionToLabelEstimator[I, O, L](node: (I, L) => (I => O)): LabelEstimator[I, O, L] = new LabelEstimator[I, O, L] {
    override def fit(v1: I, v2: L): PipelineNode[I, O] = node(v1, v2)
  }

  /**
   * Thanks to this implicit conversion, if the output of a node is an RDD you can inline a method f
   * to map on the output RDD using "node.thenMap(f)"
   */
  implicit def nodeToRDDOutHelpers[I, O](node: I => RDD[O]): RDDOutputFunctions[I, O] = new RDDOutputFunctions(node)
  class RDDOutputFunctions[I, O](node: I => RDD[O]) {
    def thenMap[U : ClassTag](f: O => U): PipelineNode[I, RDD[U]] = node.andThen(_.map(f))
  }

  /**
   * Transformers extend PipelineNode[RDD[A], RDD[B]] and exist just to reduce a bit of implementation boilerplate
   * @tparam Input input type of the transformer
   * @tparam Output output type of the transformer
   */
  abstract class Transformer[Input, Output] extends PipelineNode[RDD[Input], RDD[Output]]
  def Transformer[I, O : ClassTag](f: I => O): Transformer[I, O] = new Transformer[I, O] {
    override def apply(v1: RDD[I]): RDD[O] = v1.map(f)
  }
}