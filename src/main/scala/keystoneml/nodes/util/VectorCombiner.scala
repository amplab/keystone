package keystoneml.nodes.util

import breeze.linalg.{DenseMatrix, DenseVector}
import keystoneml.workflow.Transformer

import scala.reflect.ClassTag

/**
 * Concats a Seq of DenseVectors into a single DenseVector.
 */
case class VectorCombiner[T : ClassTag]()(implicit zero: breeze.storage.Zero[T])
    extends Transformer[Seq[DenseVector[T]], DenseVector[T]] {
  def apply(in: Seq[DenseVector[T]]): DenseVector[T] = DenseVector.vertcat(in:_*)
}
