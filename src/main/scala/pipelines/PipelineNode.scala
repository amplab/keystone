package pipelines

import java.io.Serializable

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * A Pipeline node takes an input of type A and produces an output of type B
 * @tparam A The type of input this node takes
 * @tparam B The type of output this node produces
 */
abstract class PipelineNode[-A, +B] extends Serializable {
  /**
   * Apply the body of this function to the input to the pipeline node
   * @param in The input to pass into this pipeline node
   * @return The output for the given input
   */
  def apply(in: A): B

  /**
   * Chains an estimator onto this pipeline node, producing a new estimator that when fit on same input type as
   * this node, chains this node with the node output by the original estimator.
   * @param est The estimator to chain onto the pipeline node
   * @return  The output estimator
   */
  def thenEstimator[C : ClassTag](est: Estimator[B, C]): Estimator[A, C] = Estimator((a: A) => this then est.fit(this(a)))

  /**
   * Chains a Label Estimator onto this pipeline node, producing a new Label Estimator that when fit on same input
   * type as this node, chains this node with the node output by the original Label Estimator.
   * @param est The label estimator to chain onto the pipeline node
   * @return  The output label estimator
   */
  def thenLabelEstimator[C : ClassTag, L : ClassTag](est: LabelEstimator[B, C, L]): LabelEstimator[A, C, L] = LabelEstimator((a: A, l: L) => this then est.fit(this(a), l))

  /**
   * Chains another pipeline node onto this pipeline node, producing a new pipeline node that applies both nodes
   * @param next The pipeline node to attach to the end of this one
   * @return The output pipeline node
   */
  def then[C : ClassTag](next: PipelineNode[B, C]): PipelineNode[A, C] = PipelineNode.chain(this, next)
}

object PipelineNode extends Serializable {
  /**
   * This constructor wraps a function in a PipelineNode.
   * @param node A function.
   * @return A PipelineNode encapsulating the function.
   */
  def apply[I, O](node: I => O): PipelineNode[I, O] = new PipelineNode[I, O] {
    override def apply(v1: I): O = node(v1)
  }

  /**
   * Chains two pipeline nodes together, producing a new pipeline node that applies both nodes
   * @param first The first node to apply
   * @param second The second node to apply
   * @return The output pipeline node
   */
  def chain[A, B, C](first: PipelineNode[A, B], second: PipelineNode[B, C]) = new PipelineNode[A, C] {
    override def apply(in: A): C = second(first(in))
  }

  /**
   * Thanks to this implicit conversion, if the output of a node is an RDD you can inline a method f
   * to map on the output RDD using "node.thenMap(f)"
   */
  implicit def nodeToRDDOutHelpers[I, O](node: PipelineNode[I, RDD[O]]): RDDOutputFunctions[I, O] = new RDDOutputFunctions(node)
  class RDDOutputFunctions[I, O](node: PipelineNode[I, RDD[O]]) {
    def thenMap[U : ClassTag](f: O => U): PipelineNode[I, RDD[U]] = node.then(Transformer(f))
  }
}
