package keystoneml.nodes.util

import breeze.linalg.{SparseVector, DenseVector, Vector}
import keystoneml.workflow.Transformer

/**
 * Transformer to convert vectors into SparseVectors.
 */
case class Sparsify[T <: Vector[Double]]() extends Transformer[T, SparseVector[Double]] {
  /**
   * Apply this Transformer to a single input item
   *
   * @param in The input item to pass into this transformer
   * @return The output value
   */
  override def apply(in: T): SparseVector[Double] = in match {
    case sparse: SparseVector[Double] => sparse
    case _ => SparseVector(in.toArray)
  }
}
