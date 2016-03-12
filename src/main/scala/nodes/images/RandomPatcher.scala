package nodes.images

import org.apache.spark.rdd.RDD

import utils.{ImageUtils, Image}
import pipelines.FunctionNode

/**
  * Extract random square patches from an image
  *
  * @param numPatches number of random patches to extract
  * @param patchSizeX size of each patch along xDim
  * @param patchSizeY size of each patch along yDim
  * @return numPatches images of size patchSizeX x patchSizeY
  */
case class RandomPatcher(
    numPatches: Int,
    patchSizeX: Int,
    patchSizeY: Int,
    seed: Long = 12334L) extends FunctionNode[RDD[Image], RDD[Image]] {

  val rnd = new scala.util.Random(seed)

  def apply(in: RDD[Image]): RDD[Image] = {
    in.flatMap { x => 
      randomPatchImage(x)
    }
  }

  def randomPatchImage(in: Image): Iterator[Image] = {
    val xDim = in.metadata.xDim
    val yDim = in.metadata.yDim

    (0 until numPatches).iterator.map { x =>
      val borderSizeX = xDim - patchSizeX
      val borderSizeY = yDim - patchSizeY
      // Pick a random int between 0 and borderSize (inclusive)
      val startX = rnd.nextInt(borderSizeX + 1)
      val endX = startX + patchSizeX
      val startY = rnd.nextInt(borderSizeY + 1)
      val endY = startY + patchSizeY

      ImageUtils.crop(in, startX, startY, endX, endY)
    }
  }

}
