package nodes.stats

import breeze.linalg.DenseVector
import breeze.numerics._
import pipelines.Transformer

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