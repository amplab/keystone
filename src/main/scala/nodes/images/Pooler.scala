package nodes.images

import breeze.linalg.DenseVector
import pipelines._
import utils.{ImageMetadata, ArrayVectorizedImage, Image}

/**
 * This node takes an image and performs pooling on regions of the image.
 *
 * NOTE: By default strides start from poolSize/2.
 *
 * @param stride x and y stride to get regions of the image
 * @param poolSize size of the patch to perform pooling on
 * @param pixelFunction function to apply on every pixel before pooling
 * @param poolFunction pooling function to use on every region.
 */
class Pooler(
    stride: Int,
    poolSize: Int,
    pixelFunction: Double => Double,
    poolFunction: DenseVector[Double] => Double)
  extends Transformer[Image, Image] with Logging {

  val strideStart = poolSize / 2

  def apply(image: Image) = {
    val xDim = image.metadata.xDim
    val yDim = image.metadata.yDim
    val numChannels = image.metadata.numChannels

    val numPoolsX = math.ceil((xDim - strideStart).toDouble / stride).toInt
    val numPoolsY = math.ceil((yDim - strideStart).toDouble / stride).toInt
    val patch = new Array[Double]( numPoolsX * numPoolsY * numChannels)

    // Start at strideStart in (x, y) and
    for (x <- strideStart until xDim by stride;
         y <- strideStart until yDim by stride) {
      // Extract the pool. Then apply the pixel and pool functions

      val pool = DenseVector.zeros[Double](poolSize * poolSize)
      val startX = x - poolSize/2
      val endX = math.min(x + poolSize/2, xDim)
      val startY = y - poolSize/2
      val endY = math.min(y + poolSize/2, yDim)

      var c = 0
      while (c < numChannels) {
        var s = startX
        while (s < endX) {
          var b = startY
          while (b < endY) {
            pool((s-startX) + (b-startY)*(endX-startX)) =
              pixelFunction(image.get(s, b, c))
            b = b + 1
          }
          s = s + 1
        }
        patch(c + (x - strideStart)/stride * numChannels +
          (y - strideStart)/stride * numPoolsX * numChannels) = poolFunction(pool)
        c = c + 1
      }
    }
    ArrayVectorizedImage(patch, ImageMetadata(numPoolsX, numPoolsY, numChannels))
  }
}
