package nodes.misc

import breeze.linalg._
import pipelines.Transformer

object MatrixVectorizer extends Transformer[DenseMatrix[Double], DenseVector[Double]] {
  def apply(in: DenseMatrix[Double]): DenseVector[Double] = in.toDenseVector
}