package pipelines

import org.apache.spark.rdd.RDD
import pipelines.Pipelines.PipelineNode

import scala.reflect.ClassTag

/**
 * Creates a chain of transformers. (This is only used internally).
 * @param a First transformer.
 * @param b Second transformer.
 * @tparam A Transformer chain input type.
 * @tparam B Intermediate type.
 * @tparam C Output type.
 */
private class TransformerChain[A : ClassTag, B, C : ClassTag](
    val a: Transformer[A, B],
    val b: Transformer[B, C]) extends Transformer[A, C] {

  /**
   * Transform an input.
   * @param in Input.
   * @return Transformed input.
   */
  override def transform(in: A): C = b.transform(a.transform(in))

  /**
   * Transform an RDD of Inputs.
   * @param in Input RDD.
   * @return RDD of transformed inputs.
   */
  override def transform(in: RDD[A]): RDD[C] = b.transform(a.transform(in))
}

/**
 * Special kind of transformer that does nothing.
 */
class IdentityTransformer[T : ClassTag] extends Transformer[T, T] {
  override def transform(in: T): T = in
  override def transform(in: RDD[T]): RDD[T] = in
}

/**
 * A transformer is a deterministic object that takes data of type A and turns it into type B.
 * It differs from a function in that it can operate on RDDs.
 *
 * @tparam A Input type.
 * @tparam B Output type.
 */
abstract class Transformer[A : ClassTag, B : ClassTag]
  extends PipelineNode[RDD[A], RDD[B]]
  with Serializable
  with PipelineStage[A, B] {

  /**
   * Takes an A and returns a B.
   *
   * @param in Input.
   * @return Output.
   */
  def transform(in: A): B

  /**
   * Takes an RDD[A] and returns an RDD[B].
   *
   * @param in Collection of A's.
   * @return Collection of B's.
   */
  def transform(in: RDD[A]): RDD[B] = in.map(transform)

  /**
   * Syntactic sugar for transform().
   *
   * @param in Collection of A's.
   * @return Collection of B's.
   */
  def apply(in: RDD[A]): RDD[B] = transform(in)

  /**
   * Append a transformer onto another transformer, forming a transformer chain.
   * This is the logical equivalent of function composition.
   *
   * @param next Second transformer.
   * @tparam C Output type of second transformer.
   * @return A new transformer chain.
   */
  def andThen[C : ClassTag](next: Transformer[B, C]): Transformer[A, C] = new TransformerChain[A, B, C](this, next)
}