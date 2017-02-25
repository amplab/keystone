package keystoneml.nodes.images

import keystoneml.utils.{ImageUtils, Image}
import keystoneml.workflow.Transformer

/**
  * Crop an input image to the given bounding box described by
  * (startX, startY, endX, endY).
  *
  * Wrapper for `ImageUtils.crop()`
  *
  * @param startX x-position (inclusive) to describe upper left corner of BB
  * @param startY y-position (inclusive) to describe upper left corner of BB
  * @param endX x-position (exclusive) to describe lower right corner of BB
  * @param endY y-position (exclusive) to describe lower right corner of BB
  * @return new image of size (endX - startX, endY - startY)
  */
case class Cropper(startX: Int, startY: Int, endX: Int, endY: Int) extends Transformer[Image,Image] {
  def apply(in: Image): Image = {
    ImageUtils.crop(in, startX, startY, endX, endY)
  }
}