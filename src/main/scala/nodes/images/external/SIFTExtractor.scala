package nodes.images.external

import breeze.linalg._
import nodes.images.SIFTExtractorInterface
import org.apache.spark.rdd.RDD
import utils.Image
import utils.external.VLFeat

/**
 * Extracts SIFT Descriptors at dense intervals at multiple scales using the vlfeat C library.
 *
 * @param stepSize Spacing between each sampled descriptor.
 * @param binSize Size of histogram bins for SIFT.
 * @param scales Number of scales at which to extract.
 */
class SIFTExtractor(val stepSize: Int = 3, val binSize: Int = 4, val scales: Int = 4)
  extends SIFTExtractorInterface {

  val descriptorSize = 128

  private def sift(ins: Iterator[Image]): Iterator[DenseMatrix[Float]] = {
    val extLib = new VLFeat

    ins.map(in => {
      val rawDescDataShort = extLib.getSIFTs(in.metadata.xDim, in.metadata.yDim,
        stepSize, binSize, scales, in.getSingleChannelAsFloatArray())
      val numCols = rawDescDataShort.length/descriptorSize
      val rawDescData = rawDescDataShort.map(s => s.toFloat)
      val mat = new DenseMatrix(descriptorSize, numCols, rawDescData)
      mat
    })
  }

  /**
   * Extract SIFTs from a bank of images.
   * @param in The input to pass into this pipeline node
   * @return The output for the given input
   */
  def apply(in: RDD[Image]): RDD[DenseMatrix[Float]] = in.mapPartitions(sift)
}
