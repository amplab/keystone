package nodes.learning

import breeze.linalg.DenseMatrix
import pipelines.Transformer

/**
 * Implements a fisher vector.
 *
 * @param gmm A trained Gaussian Mixture Model
 */
class BensFisherVector(
      gmm: GaussianMixtureModel)
    extends Transformer[DenseMatrix[Double], DenseMatrix[Double]] {

  override def apply(in: DenseMatrix[Double]): DenseMatrix[Double] = {
    null
  }
}
