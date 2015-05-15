package nodes.misc

import breeze.linalg._
import pipelines.Transformer

/**
 * Flattens a matrix into a vector.
 */
object MatrixVectorizer extends Transformer[DenseMatrix[Double], DenseVector[Double]] {
  def apply(in: DenseMatrix[Double]): DenseVector[Double] = in.toDenseVector
}