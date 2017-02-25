package keystoneml.nodes.images

import keystoneml.pipelines._
import keystoneml.utils.{ChannelMajorArrayVectorizedImage, Image}
import keystoneml.workflow.Transformer

case class SymmetricRectifier(maxVal: Double = 0.0, alpha: Double = 0.0)
  extends Transformer[Image, Image] {

  def apply(img: Image): Image = {
    val res = ChannelMajorArrayVectorizedImage(
      new Array[Double](img.metadata.xDim * img.metadata.yDim * img.metadata.numChannels * 2),
      img.metadata.copy(numChannels = img.metadata.numChannels * 2))

    var x, y, c = 0
    while (x < img.metadata.xDim) {
      y = 0
      while (y < img.metadata.yDim) {
        c = 0
        while (c < img.metadata.numChannels) {
          res.put(x, y, c, math.max(maxVal, img.get(x, y, c) - alpha))
          res.put(x, y, c + img.metadata.numChannels, math.max(maxVal, -img.get(x, y, c) - alpha))
          c += 1
        }
        y += 1
      }
      x += 1
    }

    res
  }
}
