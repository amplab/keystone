package nodes.images

import breeze.linalg.DenseMatrix
import workflow.Transformer
import utils.Image

/**
 * Abstract interface for SIFT extractor.
 */
trait SIFTExtractorInterface extends Transformer[Image, DenseMatrix[Float]]