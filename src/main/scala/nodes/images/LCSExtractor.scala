package nodes.images

import breeze.linalg._

import workflow.Transformer
import utils.ChannelMajorArrayVectorizedImage
import utils.Image
import utils.ImageUtils

/**
 * Computes the local color statistic of (LCS) on a regular spaced grid [1]:
 * "...each patch is also subdivided into 16 square sub-regions and each sub-region
 * is described with the means and standard deviations of the 3 RGB
 * channels, which leads to a 96 dimensional feature vector."
 * 
 * [1] Clinchant S, Csurka G, Perronnin F, Renders JM  XRCE's
 * participation to ImageEval. In: ImageEval Workshop at CVIR. 2007
 *
 * Based on code by Ben Recht <brecht@cs.berkeley.edu>
 *
 * @param stride Stride for keypoints on the grid
 * @param strideStart Starting offset for the keypoints
 * @param subPatchSize Size of neighborhood for each keypoint is -2*subPatchSize to subPatchSize
 */
class LCSExtractor(
    val stride: Int,
    val strideStart: Int,
    val subPatchSize: Int)
  extends Transformer[Image, DenseMatrix[Float]] {

  // Updates sq in place to avoid memory allocation
  def getSd(sq: Image, means: Image) = {
    require(sq.metadata.numChannels == 1)
    var i = 0
    while (i < sq.metadata.xDim) {
      var j = 0
      while (j < sq.metadata.yDim) {
        val sqOld = sq.get(i, j, 0)
        val mOld = means.get(i, j, 0)
        val px = math.sqrt(math.max(sqOld - mOld * mOld, 0))
        sq.put(i, j, 0, px)
        j = j + 1
      }
      i = i + 1
    }
    sq
  }

  def apply(image: Image): DenseMatrix[Float] = {
    val onesVector = Array.fill(subPatchSize)(1.0 / subPatchSize)

    val xDim = image.metadata.xDim
    val yDim = image.metadata.yDim
    val numChannels = image.metadata.numChannels
    
    // Typically this is (256 - 29 * 2) / 4 = 50
    val xPoolRange = strideStart until (xDim - strideStart) by stride
    val yPoolRange = strideStart until (yDim - strideStart) by stride
    val numPoolsX = xPoolRange.length
    val numPoolsY = yPoolRange.length

    // Typically -2*6 + 3 -1 = -10
    val subPatchStart = -2*subPatchSize + subPatchSize/2 - 1
    // Typically 6 + 3 - 1 = 8
    val subPatchEnd = subPatchSize + subPatchSize/2 - 1
    val subPatchStride = subPatchSize
    val subPatchRange = subPatchStart to subPatchEnd by subPatchStride

    // Typically this is (8 - (-10)) / 6  = 4 
    val numNeighborhoodX = subPatchRange.length 
    val numNeighborhoodY = subPatchRange.length

    // Typically this is 4 * 4 * 3 * 2 = 96
    val numLCSValues = numNeighborhoodX * numNeighborhoodY * numChannels * 2

    // Get means, stds for every channel
    val channelSplitImgs = ImageUtils.splitChannels(image)
    val channelSplitImgsSq = channelSplitImgs.map { img => 
      ImageUtils.mapPixels(img, x => x * x)
    }

    val means = new Array[Image](numChannels)
    val stds = new Array[Image](numChannels)
    var c = 0
    while (c < numChannels) {
      val conv = ImageUtils.conv2D(channelSplitImgs(c), onesVector, onesVector)
      means(c) = conv
      val sq = ImageUtils.conv2D(channelSplitImgsSq(c), onesVector, onesVector)
      stds(c) = getSd(sq, means(c))
      c = c + 1
    }

    val lcsValues = new DenseMatrix[Float](numLCSValues, numPoolsX * numPoolsY)

    var lcsIdx = 0
    // Start at strideStart in (x, y) and  
    for (x <- strideStart until (xDim - strideStart) by stride;
         y <- strideStart until (yDim - strideStart) by stride) {
  
      // This is our keyPoint
      val xPos = x 
      val yPos = y

      // Get keypoint ids
      val xKeyPoint = (xPos - strideStart)/stride
      val yKeyPoint = (yPos - strideStart)/stride

      // For each channel, get the neighborhood means and std. deviations
      var c = 0
      lcsIdx = 0
      while (c < numChannels) {

        // For this xPos, yPos get means, stdevs of all neighbors
        for (nx <- subPatchRange; 
             ny <- subPatchRange) {
          // lcsValues(lcsIdx) =  means(c).get((xPos + nx), (yPos + ny), 0)
          lcsValues(lcsIdx, xKeyPoint * numPoolsY + yKeyPoint) =
            means(c).get((xPos + nx), (yPos + ny), 0).toFloat
          lcsIdx = lcsIdx + 1
          lcsValues(lcsIdx, xKeyPoint * numPoolsY + yKeyPoint) =
            stds(c).get((xPos + nx), (yPos + ny), 0).toFloat
          lcsIdx = lcsIdx + 1
        }

        c = c + 1
      }
    }
    lcsValues
  }
}
