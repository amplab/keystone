package nodes.images

import pipelines.Transformer
import utils.{ImageUtils, Image}

/**
 * Converts an input images to NTSC-standard grayscale.
 */
case object GrayScaler extends Transformer[Image,Image] {
  def apply(in: Image): Image = ImageUtils.toGrayScale(in)
}
