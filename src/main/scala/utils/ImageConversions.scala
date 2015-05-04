package utils

import java.awt.image.{DataBufferByte, BufferedImage}

object ImageConversions {
  /**
   * Copied in small part from Mota's code here:
   *   http://stackoverflow.com/a/9470843
   */
  def bufferedImageToWrapper(image: BufferedImage): Image = {
    val pixels = image.getRaster().getDataBuffer().asInstanceOf[DataBufferByte].getData()
    val xDim = image.getHeight()
    val yDim = image.getWidth()
    val hasAlphaChannel = image.getAlphaRaster() != null
    val numChannels = image.getType() match {
      case BufferedImage.TYPE_3BYTE_BGR => 3
      case BufferedImage.TYPE_4BYTE_ABGR => 4
      case BufferedImage.TYPE_4BYTE_ABGR_PRE => 4
      case BufferedImage.TYPE_BYTE_GRAY => 1
      case _ => throw new RuntimeException("Unexpected Image Type " + image.getType())
    }
    val metadata = ImageMetadata(xDim, yDim, numChannels)
    ByteArrayVectorizedImage(pixels, metadata)
  }

  def grayScaleImageToWrapper(image: BufferedImage): Image = {
    val pixels = image.getRaster().getDataBuffer().asInstanceOf[DataBufferByte].getData()
    val xDim = image.getHeight()
    val yDim = image.getWidth()
    val numChannels = 3
    val metadata = ImageMetadata(xDim, yDim, numChannels)

    // Concatenate the grayscale image thrice to get three channels.
    // TODO(shivaram): Is this the right thing to do ?
    val allPixels = pixels.flatMap(p => Seq(p, p, p))
    ByteArrayVectorizedImage(allPixels, metadata)
  }

}