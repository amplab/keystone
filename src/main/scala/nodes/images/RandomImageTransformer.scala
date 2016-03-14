package nodes.images

import org.apache.spark.rdd.RDD

import utils.{ImageUtils, Image}
import workflow.Transformer

/**
 * Transform an image with the given probability
 *
 * @param chance probability that an image should be transformed
 * @param transform function to apply to image
 * @return transformed image or original image
 */

case class RandomImageTransformer(
    chance: Double,
    transform: Image => Image,
    seed: Long = 12334L) extends Transformer[Image, Image] {

  val rnd = new java.util.Random(seed)

  def apply(im: Image): Image = {
    val flip = rnd.nextDouble()
    if (flip < chance) {
      transform(im)
    } else {
      im
    }
  }
}
