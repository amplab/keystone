package nodes.images

import breeze.linalg.DenseMatrix
import pipelines.Transformer

/**
 * Abstract interface for Fisher Vector.
 */
trait FisherVectorInterface extends Transformer[DenseMatrix[Double], DenseMatrix[Double]]