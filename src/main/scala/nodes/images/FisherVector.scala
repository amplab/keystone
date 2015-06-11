package nodes.images

import breeze.linalg.DenseMatrix
import workflow.Transformer

/**
 * Abstract interface for Fisher Vector.
 */
trait FisherVectorInterface extends Transformer[DenseMatrix[Float], DenseMatrix[Float]]