package nodes.images

import pipelines.Transformer
import utils.{ImageUtils, Image}


/**
 * Rescales an input image from [0 .. 255] to [0 .. 1]. Works by dividing each pixel by 255.0.
 */
object PixelScaler extends Transformer[Image,Image] {
  def apply(im: Image): Image = {
    ImageUtils.mapPixels(im, _/255.0)
  }
}