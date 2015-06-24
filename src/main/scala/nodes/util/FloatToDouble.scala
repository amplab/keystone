package nodes.util

import breeze.linalg._
import workflow.Transformer

/**
 * Converts float matrix to a double matrix.
 */
object FloatToDouble extends Transformer[DenseMatrix[Float], DenseMatrix[Double]] {
  def apply(in: DenseMatrix[Float]): DenseMatrix[Double] = convert(in, Double)
}
