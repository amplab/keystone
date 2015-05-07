package nodes.math

import breeze.linalg.DenseVector
import pipelines._

/**
 * Adds a ones vector to the beginning of a Dense Vector.
 */
case object InterceptAdder extends Transformer[DenseVector[Double], DenseVector[Double]] {
  def apply(in: DenseVector[Double]): DenseVector[Double] = {
    DenseVector.vertcat(DenseVector(1.0), in)
  }
}