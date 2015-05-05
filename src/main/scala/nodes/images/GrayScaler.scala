package nodes.images

import org.apache.spark.rdd.RDD
import pipelines.Transformer
import utils.{ImageUtils, Image}

/**
 * Converts an input images to NTSC-standard grayscale.
 */
class GrayScaler extends Transformer[Image,Image] {

  def apply(in: RDD[Image]): RDD[Image] = {
    in.map(ImageUtils.toGrayScale)
  }

  override def apply(in: Image): Image = ImageUtils.toGrayScale(in)
}