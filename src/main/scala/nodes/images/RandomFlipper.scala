package nodes.images

import org.apache.spark.rdd.RDD

import utils.{ImageUtils, Image}
import workflow.Transformer

/**
  * Flips an image horizontally with given probability
  *
  * @param flipChance probability that an image should be flipped
  * @return image flipped image or original image
  */
case class RandomFlipper(flipChance: Double, seed: Long = 12334L) extends Transformer[Image, Image] {

  val rnd = new java.util.Random(seed)

  def apply(im: Image): Image = {
    val flip = rnd.nextDouble()
    if (flip < flipChance) {
      ImageUtils.flipHorizontal(im)
    } else {
      im
    }
  }
}
