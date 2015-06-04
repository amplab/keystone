package nodes.stats

import breeze.linalg.{DenseVector, DenseMatrix}
import breeze.numerics._
import workflow.Transformer

/**
 *  Apply power normalization: z <- sign(z)|z|^{\rho}
 *  with \rho = \frac{1}{2}
 *  This a "signed square root"
 */
object SignedHellingerMapper extends Transformer[DenseVector[Double], DenseVector[Double]] {
  def apply(in: DenseVector[Double]): DenseVector[Double] = {
    signum(in) :* sqrt(abs(in))
  }
}

object BatchSignedHellingerMapper extends Transformer[DenseMatrix[Float], DenseMatrix[Float]] {
  def apply(in: DenseMatrix[Float]): DenseMatrix[Float] = {
    in.map(x => (math.signum(x) * math.sqrt(math.abs(x))).toFloat)
  }
}
