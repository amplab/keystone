package nodes.images

import org.apache.spark.rdd.RDD

import utils.{ImageUtils, Image}
import pipelines.FunctionNode

/**
  * Extract four corner patches and the center patch of the specified size.
  * If flips is set to true, then horizontal flips of all 5 patches is also
  * returned
  *
  * @param patchSizeX size of patch along xDim
  * @param patchSizeY size of patch along yDim
  * @param horizontalFlips if horizontal flips of patches should also be returned
  * @return patches of size patchSizeX x patchSizeY
  */
case class CenterCornerPatcher(
    patchSizeX: Int,
    patchSizeY: Int,
    horizontalFlips: Boolean) extends FunctionNode[RDD[Image], RDD[Image]] {

  def apply(in: RDD[Image]): RDD[Image] = {
    in.flatMap { x => 
      centerCornerPatchImage(x)
    }
  }

  def centerCornerPatchImage(in: Image): Iterator[Image] = {
    val xDim = in.metadata.xDim
    val yDim = in.metadata.yDim

    val startXs = Array(0, xDim-patchSizeX, 0, xDim-patchSizeX, (xDim-patchSizeX)/2)
    val startYs = Array(0, 0, yDim-patchSizeY, (yDim-patchSizeY)/2)

    (0 until startXs.length).iterator.flatMap { idx =>
      val endX = startXs(idx) + patchSizeX
      val endY = startYs(idx) + patchSizeY
      val im = ImageUtils.crop(in, startXs(idx), startYs(idx), endX, endY)
      if (horizontalFlips) {
        val flippedIm = ImageUtils.flipHorizontal(im) 
        Iterator(im, flippedIm)
      } else {
        Iterator.single(im)
      }
    }
  }
}
