package keystoneml.nodes.images

import breeze.linalg.DenseMatrix
import keystoneml.workflow.Transformer
import keystoneml.utils.Image

/**
 * Abstract interface for SIFT extractor.
 */
trait SIFTExtractorInterface extends Transformer[Image, DenseMatrix[Float]]