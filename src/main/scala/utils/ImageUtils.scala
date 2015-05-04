package utils

import java.awt.image.BufferedImage
import java.io.InputStream
import javax.imageio.ImageIO

import pipelines.Logging

object ImageUtils extends Logging {

  /**
   * Load image from file.
   * @param fileBytes Bytes of an input file.
   * @return
   */
  def loadImage(fileBytes: InputStream): Option[Image] = {
    classOf[ImageIO].synchronized {
      try {
        val img = ImageIO.read(fileBytes)
        if (img != null) {
          if (img.getHeight() < 36 || img.getWidth() < 36) {
            logWarning(s"Ignoring SMALL IMAGE ${img.getHeight}x${img.getWidth()}")
            None
          } else {
            if (img.getType() == BufferedImage.TYPE_3BYTE_BGR) {
              val imgW = ImageConversions.bufferedImageToWrapper(img)
              Some(imgW)
            } else if (img.getType() == BufferedImage.TYPE_BYTE_GRAY) {
              val imgW = ImageConversions.grayScaleImageToWrapper(img)
              Some(imgW)
            } else {
              logWarning(s"Ignoring image, not RGB or Grayscale of type ${img.getType}")
              None
            }
          }
        } else {
          logWarning(s"Failed to parse image, (result was null)")
          None
        }
      } catch {
        case e: Exception =>
          logWarning(s"Failed to parse image: due to ${e.getMessage}")
          None
      }
    }
  }


  def toGrayScale(in: Image): Image = {
    //From the Matlab docs for rgb2gray:
    //rgb2gray converts RGB values to grayscale values by forming a weighted sum of the R, G, and B components:
    //0.2989 * R + 0.5870 * G + 0.1140 * B

    val numChannels = in.metadata.numChannels
    val out = new ArrayVectorizedImage(new Array(in.metadata.xDim * in.metadata.yDim),
      ImageMetadata(in.metadata.xDim, in.metadata.yDim, 1))
    var i = 0
    while (i < in.metadata.xDim) {
      var j = 0
      while (j < in.metadata.yDim) {
        var sumSq = 0.0
        var k = 0
        if (numChannels == 3) {
          //Assume data is in RGB order. Todo - we should check the metadata for this.
          val px = 0.2989 * in.get(i, j, 2) + 0.5870 * in.get(i, j, 1) + 0.1140 * in.get(i, j, 0)
          //val px = 0.2989 * in.get(i, j, 0) + 0.5870 * in.get(i, j, 1) + 0.1140 * in.get(i, j, 2)
          out.put(i, j, 0, px)
        }
        else {
          while (k < numChannels) {
            sumSq = sumSq + (in.get(i, j, k) * in.get(i, j, k))
            k = k + 1
          }
          val px = math.sqrt(sumSq/numChannels)
          out.put(i, j, 0, px)
        }
        j = j + 1
      }
      i = i + 1
    }
    out
  }
}
