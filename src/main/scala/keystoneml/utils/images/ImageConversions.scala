package keystoneml.utils

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


  /**
    * Converts an image to a buffered image.
    * If Image is non-standard (that is, values not in (0,255), the "scale"
    * argument can be passed. Currently assumes a 3 or 1-dimensional image.
    * @param im An Image.
    * @param scale Boolean indicating whether to scale or not.
    * @return
    */
  def imageToBufferedImage(im: Image, scale: Boolean=false): BufferedImage = {
    val canvas = new BufferedImage(im.metadata.yDim, im.metadata.xDim, BufferedImage.TYPE_INT_RGB)

    //Scaling
    val scalef: Double => Int = if (scale) {
      val immin = im.toArray.min
      val immax = im.toArray.max
      d: Double => (255*(d-immin)/immax).toInt
    } else {
      d: Double => d.toInt
    }

    var x = 0
    while (x < im.metadata.xDim) {
      var y = 0
      while (y < im.metadata.yDim) {

        //Scale and pack into an rgb pixel.
        val chanArr = im.metadata.numChannels match {
          case 1 => Array(0,0,0)
          case 3 => Array(0,1,2)
        }

        val pixVals = chanArr.map(c => im.get(x, y, c)).map(scalef)
        val pix = (pixVals(0) << 16) | (pixVals(1) << 8) | pixVals(2)

        //Note, BufferedImage has opposite canvas coordinate system from us.
        //E.g. their x,y is our y,x.
        canvas.setRGB(y, x, pix)
        y += 1
      }
      x += 1
    }

    canvas
  }

}