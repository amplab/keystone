package keystoneml.nodes.util

import breeze.linalg.DenseVector
import breeze.linalg.Vector
import keystoneml.workflow.Transformer

/**
 * Transformer to densify vectors into DenseVectors.
 */
case class Densify[T <: Vector[Double]]() extends Transformer[T, DenseVector[Double]] {
  /**
   * Apply this Transformer to a single input item
   *
   * @param in The input item to pass into this transformer
   * @return The output value
   */
  override def apply(in: T): DenseVector[Double] = in match {
    case dense: DenseVector[Double] => dense
    case _ => in.toDenseVector
  }
}
