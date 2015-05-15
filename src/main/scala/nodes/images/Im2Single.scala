package nodes.images

import pipelines.Transformer
import utils.{ImageUtils, Image}

object Im2Single extends Transformer[Image,Image] {
  def apply(im: Image): Image = {
    ImageUtils.mapPixels(im, _/255.0)
  }
}