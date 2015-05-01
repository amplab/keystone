package utils

import java.awt.image.{BufferedImage, DataBufferByte}
import java.io.{InputStream, File, FileInputStream}
import javax.imageio.ImageIO

import sys.process._

import pipelines._

/**
 * A wrapper trait for images that might be stored in various ways.  Be warned
 * that using this wrapper probably introduces some inefficiency.  Also, images
 * are currently treated as immutable, which may introduce a serious
 * performance problem; in the future we may need to add a set() method.
 *
 * If you have a choice and performance matters to you, use
 * ArrayVectorizedImage, as it is likely to be the most efficient
 * implementation.
 */
trait Image {
  val metadata: ImageMetadata

  /** Get the pixel value at (x, y, channelIdx).  Channels are indexed as
    * follows:
    *   - If the image is RGB, 0 => blue, 1 => green, 2 => red.
    *   - If the image is RGB+alpha, 0 => blue, 1=> green, 2 => red, and
    *     3 => alpha.
    *   - Other channel schemes are unsupported; the only reason this matters
    *     is that input converters (e.g. from BufferedImage to Image) need to
    *     handle channels consistently.
    */
  def get(x: Int, y: Int, channelIdx: Int): Double

  /**
   * Put a pixel value at (x, y, channelIdx).
   */
  def put(x: Int, y: Int, channelIdx: Int, newVal: Double)

  /** Returns a flat version of the image, represented as a single array.
    * It is indexed as follows: The pixel value for (x, y, channelIdx)
    * is at channelIdx + x*numChannels + y*numChannels*xDim.
    *
    * This implementation works for arbitrary image formats but it is
    * inefficient.
    */
  def toVector: Array[Double] = {
    val flat = new Array[Double](this.flatSize)
    var y = 0
    while (y < this.metadata.yDim) {
      val runningOffsetY = y*this.metadata.numChannels*this.metadata.xDim
      var x = 0
      while (x < this.metadata.xDim) {
        val runningOffsetX = runningOffsetY + x*this.metadata.numChannels
        var channelIdx = 0
        while (channelIdx < this.metadata.numChannels) {
          flat(channelIdx + runningOffsetX) = get(x, y, channelIdx)
          channelIdx += 1
        }
        x += 1
      }
      y += 1
    }
    flat
  }

  def getSingleChannelAsIntArray() : Array[Int] = {
    if (this.metadata.numChannels > 1) {
      return null;
    }
    var index = 0;
    var flat = new Array[Int](this.metadata.xDim*this.metadata.yDim)
    (0 until metadata.xDim).map({ x =>
      (0 until metadata.yDim).map({ y =>
        val px = get(x, y, 0);
        if(px < 1) {
          flat(index) = (255*px).toInt
        }
        else {
          flat(index) = math.round(px).toInt
        }
        index += 1
      })
    })
    flat
  }

  def getSingleChannelAsFloatArray() : Array[Float] = {
    if (this.metadata.numChannels > 1) {
      return null;
    }
    var index = 0;
    var flat = new Array[Float](this.metadata.xDim*this.metadata.yDim)
    (0 until metadata.yDim).map({ y =>
      (0 until metadata.xDim).map({ x =>
        flat(index) = get(x, y, 0).toFloat
        index += 1
      })
    })
    flat
  }

  def flatSize: Int = {
    metadata.numChannels*metadata.xDim*metadata.yDim
  }

  def prettyPrint: String = {
    (0 until metadata.yDim).map({y =>
      (0 until metadata.xDim).map({x =>
        "[" + (0 until metadata.numChannels).map({channelIdx =>
          "%.3f".format(get(x, y, channelIdx))
        }).mkString(",") + "]"
      }).mkString(" ")
    }).mkString("\n")
  }
  // Added by Gylfi to get printed image in pgm format
  // If Image only has one channel, i.e. GrayScale, a pgm P2 (ASCII) format is printed
  // NOTE that xDim is the height and yDim is the width
  //*
  override def toString: String = {
    if (metadata.numChannels == 1) {
      // Print image as pgm text format
      // Find the max pixel value first
      var max = 0d
      (0 until metadata.xDim).map({x =>
        (0 until metadata.yDim).map({y =>
          val pix = get(x,y,0)
          if ( pix > max) {
            max = pix
          }
        })
      })
      // Create the pgm header and then print the pixel values
      "P2\n" + metadata.yDim.toString() + " " + metadata.xDim.toString() + "\n" + "%1.0f\n".format(max) +
        (0 until metadata.xDim).map({x =>
          (0 until metadata.yDim).map({y =>
            "%1.0f".format(get(x, y, 0))
          }).mkString(" ")
        }).mkString("\n")
    }
    else {
      "(" + metadata.yDim.toString() + "," + metadata.xDim.toString() + ")" +
        (0 until metadata.xDim).map({x =>
          (0 until metadata.yDim).map({y =>
            "[" + (0 until metadata.numChannels).map({channelIdx =>
              "%.3f".format(get(x, y, channelIdx))
            }).mkString(",") + "]"
          }).mkString(" ")
        }).mkString("\n")
    }
  }
  //} // */

  /** An inefficient implementation of equals().  Subclasses should override
    * this if they can implement it more cheaply and anyone cares about such
    * things.
    */
  override def equals(o: Any): Boolean = {
    if (o == null || !o.isInstanceOf[Image]) {
      false
    } else {
      val other = o.asInstanceOf[Image]
      if (!this.metadata.equals(other.metadata)) {
        false
      } else {
        for (xIdx <- (0 until metadata.xDim);
             yIdx <- (0 until metadata.yDim);
             channelIdx <- (0 until metadata.numChannels)) {
          if (this.get(xIdx, yIdx, channelIdx) != other.get(xIdx, yIdx, channelIdx)) {
            return false
          }
        }
        true
      }
    }
  }
}

/**
 * Contains metadata about the storage format of an image.
 *
 * @param xDim is the height of the image(!)
 * @param yDim is the width of the image
 * @param numChannels is the number of color channels in the image
 */
case class ImageMetadata(xDim: Int, yDim: Int, numChannels: Int)

// Not as performant as the vectorized implementations.
case class DeepArrayImage(
                           deepArray: Array[Array[Array[Double]]],
                           override val metadata: ImageMetadata) extends Image {
  override def get(x: Int, y: Int, channelIdx: Int) = deepArray(x)(y)(channelIdx)

  override def toVector = {
    throw new Exception("toVector not implemented!")
  }


  override def put(x: Int, y: Int, channelIdx: Int, newVal: Double) = {
    throw new Exception("put not implemented!")
  }
}

/**
 * @vectorizedImage is indexed as follows: The pixel value for
 * (x, y, channelIdx) is at channelIdx + x*numChannels + y*numChannels*xDim.
 */
case class ArrayVectorizedImage(
                                 vectorizedImage: Array[Double],
                                 override val metadata: ImageMetadata) extends VectorizedImage {
  override def imageToVectorCoords(x: Int, y: Int, channelIdx: Int): Int = {
    channelIdx + x*metadata.numChannels + y*metadata.xDim*metadata.numChannels
  }

  override def getInVector(vectorIdx: Int) = vectorizedImage(vectorIdx)


  override def putInVector(vectorIdx: Int, newVal: Double) = {
    vectorizedImage(vectorIdx) = newVal
  }

  override def toVector = vectorizedImage
}

/**
 * @vectorizedImage is indexed as follows: The pixel value for
 * (x, y, channelIdx) is at x + y*xDim + channelIdx*xDim*yDim.
 */
case class InverseIndexedArrayVectorizedImage(
                                               vectorizedImage: Array[Double],
                                               override val metadata: ImageMetadata) extends VectorizedImage {
  override def imageToVectorCoords(x: Int, y: Int, channelIdx: Int): Int = {
    x + y*metadata.xDim + channelIdx*metadata.xDim*metadata.yDim
  }

  override def getInVector(vectorIdx: Int) = vectorizedImage(vectorIdx)

  /*override def toVector = {

  }*/

  override def putInVector(vectorIdx: Int, newVal: Double) = {
    vectorizedImage(vectorIdx) = newVal
  }
}

/**
 * Wraps a byte array, where a byte is a color channel value.  This is the
 * format generated by Java's JPEG parser.
 *
 * @vectorizedImage is indexed as follows: The pixel value for (x, y, channelIdx)
 *   is at channelIdx + y*numChannels + x*numChannels*yDim.
 */
case class ByteArrayVectorizedImage(
                                     vectorizedImage: Array[Byte],
                                     override val metadata: ImageMetadata) extends VectorizedImage {
  override def imageToVectorCoords(x: Int, y: Int, channelIdx: Int): Int = {
    channelIdx + y*metadata.numChannels + x*metadata.yDim*metadata.numChannels
  }

  // FIXME: This is correct but inefficient - every time we access the image we
  // use several method calls (which are hopefully inlined) and a conversion
  // from byte to double (which hopefully at least does not involve any
  // boxing).
  override def getInVector(vectorIdx: Int) = {
    val signedValue = vectorizedImage(vectorIdx)
    if (signedValue < 0) {
      signedValue + 256
    } else {
      signedValue
    }
  }

  override def putInVector(vectorIdx: Int, newVal: Double) = {
    throw new Exception("putInVector not implemented!")
  }
}

/**
 * Wraps a double array.
 *
 * @vectorizedImage is indexed as follows: The pixel value for (x, y, channelIdx)
 *   is at y + x.metadata.yDim + channelIdx*metadata.yDim*metadata.xDim
 *   */
case class RowColumnMajorByteArrayVectorizedImage(
                vectorizedImage: Array[Byte],
                override val metadata: ImageMetadata) extends VectorizedImage {
  override def imageToVectorCoords(x: Int, y: Int, channelIdx: Int): Int = {
    val cidx = channelIdx/* match { //Todo: Temporary fix to make channel indexes match nick's code.
      case 0 => 2
      case 2 => 0
      case n => n
    }*/
    y + x*metadata.yDim + cidx*metadata.yDim*metadata.xDim
  }

  // FIXME: This is correct but inefficient - every time we access the image we
  // use several method calls (which are hopefully inlined) and a conversion
  // from byte to double (which hopefully at least does not involve any
  // boxing).
  override def getInVector(vectorIdx: Int) = {
    val signedValue = vectorizedImage(vectorIdx)
    if (signedValue < 0) {
      signedValue + 256
    } else {
      signedValue
    }
  }

  override def putInVector(vectorIdx: Int, newVal: Double) = ???
}

/**
 * Wraps a byte array, where a byte is a color channel value.
 *
 * @vectorizedImage is indexed as follows: The pixel value for (x, y, channelIdx)
 *   is at y + x.metadata.yDim + channelIdx*metadata.yDim*metadata.xDim
 *   */
case class RowColumnMajorArrayVectorizedImage(
                                               vectorizedImage: Array[Double],
                                               override val metadata: ImageMetadata) extends VectorizedImage {
  override def imageToVectorCoords(x: Int, y: Int, channelIdx: Int): Int = {
    val cidx = channelIdx/* match { //Todo: Temporary fix to make channel indexes match nick's code.
      case 0 => 2
      case 2 => 0
      case n => n
    }*/
    y + x*metadata.yDim + cidx*metadata.yDim*metadata.xDim
  }

  // FIXME: This is correct but inefficient - every time we access the image we
  // use several method calls (which are hopefully inlined) and a conversion
  // from byte to double (which hopefully at least does not involve any
  // boxing).
  override def getInVector(vectorIdx: Int) = {
    vectorizedImage(vectorIdx)
  }

  override def putInVector(vectorIdx: Int, newVal: Double) = {
    vectorizedImage(vectorIdx) = newVal
  }

  // override def toVector = vectorizedImage
}

/** Helper trait for implementing Images that wrap vectorized representations
  * of images.
  */
trait VectorizedImage extends Image {
  def imageToVectorCoords(x: Int, y: Int, channelIdx: Int): Int

  def getInVector(vectorIdx: Int): Double

  def putInVector(vectorIdx: Int, newVal: Double): Unit

  override def get(x: Int, y: Int, channelIdx: Int) = {
    getInVector(imageToVectorCoords(x, y, channelIdx))
  }

  override def put(x: Int, y: Int, channelIdx: Int, newVal: Double) = {
    putInVector(imageToVectorCoords(x, y, channelIdx), newVal)
  }
}

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


  def cifar10ToBufferedImage(cifar: Array[Byte]): RowColumnMajorByteArrayVectorizedImage = {
    val nrow = 32
    val ncol = 32
    val nchan = 3
    val rowlen = ncol*nchan

    val byteLen = nrow*rowlen

    //Allocate some space for the rows.

    assert(cifar.length == byteLen, "CIFAR-10 Images MUST be 32x32x3.")

    RowColumnMajorByteArrayVectorizedImage(cifar, ImageMetadata(nrow, ncol, nchan))
  }


  /**
   * Converts an image to a buffered image.
   * If Image is non-standard (that is, values not in (0,255), the "scale"
   * argument can be passed. Currently assumes a 3 or 1-dimensional image.
   * @param im An Image.
   * @param scale Boolean indicating whether to scale or not.
   * @return
   */
  def imageToBufferedImage(im: Image, scale: Double=1.0): BufferedImage = {
    val canvas = new BufferedImage(im.metadata.yDim, im.metadata.xDim, BufferedImage.TYPE_INT_RGB)

    val chanArr = im.metadata.numChannels match {
      case 1 => Array(0,0,0)
      case 3 => Array(2,1,0)
    }

    var x = 0
    while (x < im.metadata.xDim) {
      var y = 0
      while (y < im.metadata.yDim) {
        var c = 0
        var pix: Int = 0
        var shift = 16
        while (c < chanArr.length) {
          pix = pix | (im.get(x, y, chanArr(c)) * scale).toInt << shift
          shift = shift - 8
          c = c + 1
        }

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

  def mapPixels(in: Image, fun: Double => Double): Image = {
    val out = new ArrayVectorizedImage(new Array[Double](in.metadata.xDim * in.metadata.yDim * in.metadata.numChannels),
      ImageMetadata(in.metadata.xDim, in.metadata.yDim, in.metadata.numChannels))

    var x, y, c = 0
    while (x < in.metadata.xDim) {
      y = 0
      while (y < in.metadata.yDim) {
        c = 0
        while (c < in.metadata.numChannels) {
          out.put(x, y, c, fun(in.get(x,y,c)))
          c+=1
        }
        y+=1
      }
      x+=1
    }

    out
  }

  def toGrayScaleNTSC(in: Image): Image = {
    // From the Matlab docs for rgb2gray:
    // rgb2gray converts RGB values to grayscale values by forming a weighted sum of the R, G, and B components:
    // 0.2989 * R + 0.5870 * G + 0.1140 * B
    val numChannels = in.metadata.numChannels
    val out = new ArrayVectorizedImage(new Array(in.metadata.xDim * in.metadata.yDim),
      ImageMetadata(in.metadata.xDim, in.metadata.yDim, 1))
    var i = 0
    while (i < in.metadata.xDim) {
      var j = 0
      while (j < in.metadata.yDim) {
        // We can either have 3, 4 or 1 channel according to our parser code right now
        if (numChannels == 3) {
          // Data is in BGR order. Todo - we should check the metadata for this.
          val px = 0.2989 * in.get(i, j, 2) + 0.5870 * in.get(i, j, 1) + 0.1140 * in.get(i, j, 0)
          out.put(i, j, 0, px/255.0f)
        } else if (numChannels == 4) {
          // Data is in ABGR order
          val px = 0.2989 * in.get(i, j, 3) + 0.5870 * in.get(i, j, 2) + 0.1140 * in.get(i, j, 1)
          out.put(i, j, 0, px/255.0f)
        } else {
          out.put(i, j, 0, in.get(i, j, 0)/255.0f)
        }
        j = j + 1
      }
      i = i + 1
    }
    out
  }

  def imageChannelUnion(in: Image, in2: Image): Image = {
    assert(in.metadata.xDim == in2.metadata.xDim &&
      in.metadata.yDim == in2.metadata.yDim,
      "Images must have the same dimension.")
    val out = new ArrayVectorizedImage(
      new Array[Double](in.metadata.xDim * in.metadata.yDim *
        (in.metadata.numChannels + in2.metadata.numChannels)),
      ImageMetadata(in.metadata.xDim, in.metadata.yDim,
        (in.metadata.numChannels + in2.metadata.numChannels)))

    var x, y, c, c1 = 0
    while (x < in.metadata.xDim) {
      y = 0
      while (y < in.metadata.yDim) {
        c = 0
        while (c < in.metadata.numChannels) {
          out.put(x, y, c, in.get(x,y,c))
          c+=1
        }
        c1 = 0
        while (c1 < in2.metadata.numChannels) {
          out.put(x, y, c, in.get(x,y,c1))
          c += 1
          c1 += 1
        }

        y+=1
      }
      x+=1
    }

    out
  }

  def pixelCombine(in: Image, in2: Image, fun: (Double, Double) => Double = _ + _): Image = {
    assert(in.metadata.xDim == in2.metadata.xDim &&
      in.metadata.yDim == in2.metadata.yDim &&
      in.metadata.numChannels == in2.metadata.numChannels,
      "Images must have the same dimension.")

    val out = new ArrayVectorizedImage(new Array[Double](in.metadata.xDim * in.metadata.yDim * in.metadata.numChannels),
      ImageMetadata(in.metadata.xDim, in.metadata.yDim, in.metadata.numChannels))

    var x, y, c = 0
    while (x < in.metadata.xDim) {
      y = 0
      while (y < in.metadata.yDim) {
        c = 0
        while (c < in.metadata.numChannels) {
          out.put(x, y, c, fun(in.get(x,y,c), in2.get(x,y,c)))
          c+=1
        }
        y+=1
      }
      x+=1
    }

    out
  }

  def splitChannels(in: Image): Array[Image] = {
    val out = new Array[Image](in.metadata.numChannels)
    var c = 0
    while (c < in.metadata.numChannels) {
      val a = ArrayVectorizedImage(
        new Array[Double](in.metadata.xDim * in.metadata.yDim),
        ImageMetadata(in.metadata.xDim, in.metadata.yDim, 1))
      var x = 0
      while (x < in.metadata.xDim) {
        var y = 0
        while (y < in.metadata.yDim) {
          a.put(x, y, 0, in.get(x, y, c))
          y = y + 1
        }
        x = x + 1
      }
      out(c) = a
      c = c + 1
    }
    out
  }

  def toGrayScale(in: Image) = {
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


  def makeString(in: Image): String = {
    def getRow(in: Image, chan: Int, row: Int): String = {
      ((0 until in.metadata.xDim).map("%2.3f" format in.get(_, row, chan))).mkString("\t")
    }

    def getChannel(in: Image, chan: Int): String = {
      ((0 until in.metadata.yDim).map(getRow(in, chan, _))).mkString("\n")
    }

    ((0 until in.metadata.numChannels).map(getChannel(in, _))).mkString("\n\n")
  }

  def conv2D(img: Image, xFilter: Array[Double], yFilter: Array[Double]): Image = {

    val paddedXDim = img.metadata.xDim + xFilter.length - 1
    val paddedYDim = img.metadata.yDim + yFilter.length - 1
    val imgPadded = new InverseIndexedArrayVectorizedImage(new Array[Double](paddedXDim * paddedYDim *
      img.metadata.numChannels), ImageMetadata(paddedXDim, paddedYDim, img.metadata.numChannels))
    // println("paddedXDim " + paddedXDim + " y " + paddedYDim)

    val xPadLow = math.floor((xFilter.length - 1).toFloat / 2).toInt
    // Since we go from 0 to paddedXDim
    val xPadHigh = (paddedXDim - 1) - math.ceil((xFilter.length - 1).toFloat / 2).toInt
    // println("xL " + xPadLow + " xH " + xPadHigh)

    val yPadLow = math.floor((yFilter.length - 1).toFloat / 2).toInt
    // Since we go from 0 to paddedYDim
    val yPadHigh = (paddedYDim - 1) - math.ceil((yFilter.length - 1).toFloat / 2).toInt
    // println("yL " + yPadLow + " yH " + yPadHigh)

    var c = 0
    while (c < img.metadata.numChannels) {
      var y = 0
      while (y < paddedYDim) {
        var yVal = -1
        if (y < yPadLow || y > yPadHigh) {
          yVal = 0
        }
        var x = 0
        while (x < paddedXDim) {
          var xVal = -1
          if (x < xPadLow || x > xPadHigh) {
            xVal = 0
          }

          var px = 0.0
          if (!(xVal == 0 || yVal == 0)) {
            px = img.get(x - xPadLow, y - yPadLow, c)
          }
          imgPadded.put(x, y, c, px)
          x = x + 1
        }
        y = y + 1
      }
      c = c + 1
    }

    val xFilterToUse = xFilter.reverse
    val yFilterToUse = yFilter.reverse
    val imgChannels = imgPadded.metadata.numChannels
    val imgWidth = imgPadded.metadata.yDim
    val imgHeight = imgPadded.metadata.xDim

    val resWidth = imgWidth - yFilterToUse.length + 1
    val resHeight = imgHeight - xFilterToUse.length + 1

    // Storage area for intermediate output.
    val midres = new RowColumnMajorArrayVectorizedImage(new Array[Double](resHeight*imgWidth*imgChannels),
      ImageMetadata(resHeight, imgWidth, imgChannels))

    // Storage for final output.
    val res = new RowColumnMajorArrayVectorizedImage(new Array[Double](resWidth*resHeight*imgChannels),
      ImageMetadata(resHeight, resWidth, imgChannels))

    // First we do the rows.
    var x = 0
    var y, chan, i = 0
    var tmp = 0.0

    while (chan < imgChannels) {
      y = 0
      while (y < imgWidth) {
        x = 0
        while (x < resHeight) {
          i = 0
          tmp = 0.0
          var idxToGet = x + y*paddedXDim + chan*paddedXDim*paddedYDim
          while (i < xFilterToUse.length) {
            tmp += imgPadded.getInVector(idxToGet + i) * xFilterToUse(i)
            i += 1
          }
          midres.put(x, y, chan, tmp)
          x += 1
        }
        y += 1
      }
      chan += 1
    }

    // Then we do the columns.
    x = 0
    y = 0
    chan = 0
    i = 0

    while (chan < imgChannels) {
      x = 0
      while (x < resHeight) {
        y = 0
        while ( y < resWidth) {
          val idxToPut = y + x*resWidth + chan*resWidth*resHeight
          var idxToGet = y + x*imgWidth + chan*imgWidth*resHeight
          i = 0
          tmp = 0.0
          while (i < yFilterToUse.length) {
            tmp += midres.getInVector(idxToGet + i) * yFilterToUse(i)
            i += 1
          }
          res.putInVector(idxToPut, tmp)
          y += 1
        }
        x += 1
      }
      chan += 1
    }
    res


  }

  /**
   * Convolves images with two one-dimensional filters.
   *
   * @param xFilter Horizontal convolution filter.
   * @param yFilter Vertical convolution filter.
   * @param imgWidth Width of images in pixels.
   * @param imgHeight Height of images in pixels.
   */
  def conv2DValid(img: Image, xFilter: Array[Double], yFilter: Array[Double]): Image = {
    val xFilterToUse = xFilter.reverse
    val yFilterToUse = yFilter.reverse
    val imgChannels = img.metadata.numChannels
    val imgWidth = img.metadata.yDim
    val imgHeight = img.metadata.xDim

    val resWidth = imgWidth - yFilterToUse.length + 1
    val resHeight = imgHeight - xFilterToUse.length + 1

    // Storage area for intermediate output.
    val midres = new RowColumnMajorArrayVectorizedImage(new Array[Double](resHeight*img.metadata.yDim*imgChannels),
      ImageMetadata(resHeight, img.metadata.yDim, imgChannels))

    // Storage for final output.
    val res = new RowColumnMajorArrayVectorizedImage(new Array[Double](resWidth*resHeight*imgChannels),
      ImageMetadata(resHeight, resWidth, imgChannels))

    // First we do the rows.
    var x, y, chan, i = 0
    var tmp = 0.0

    while (chan < imgChannels) {
      x = 0
      while (x < resHeight) {
        y = 0
        while (y < img.metadata.yDim) {
          i = 0
          tmp = 0.0
          while (i < xFilterToUse.length) {
            tmp += img.get(x+i, y, chan) * xFilterToUse(i)
            i += 1
          }
          midres.put(x, y, chan, tmp)
          y += 1
        }
        x += 1
      }
      chan += 1
    }

    // Then we do the columns.
    x = 0
    y = 0
    chan = 0
    i = 0

    while (chan < imgChannels) {
      x = 0
      while (x < resHeight) {
        y = 0
        while ( y < resWidth) {
          val idxToPut = y + x*resWidth + chan*resWidth*resHeight
          var idxToGet = y + x*imgWidth + chan*imgWidth*resHeight
          i = 0
          tmp = 0.0
          while (i < yFilterToUse.length) {
            tmp += midres.getInVector(idxToGet + i) * yFilterToUse(i)
            i += 1
          }
          res.putInVector(idxToPut, tmp)
          y += 1
        }
        x += 1
      }
      chan += 1
    }
    res
  }

  def writeImage(fname: String, in: Image) = {
    val bi = ImageConversions.imageToBufferedImage(in)
    val outf = new File(fname)
    ImageIO.write(bi, "png", outf)
  }

  def resizeJMagick(in: Image, ratio: Double, scale: Double=1.0): Image = {
    val ext = "bmp"

    val fnamePrefix = "/tmp/testImg" + scala.util.Random.nextInt(1000) +  "-" + System.nanoTime()
    val fname = fnamePrefix + "." + ext
    val bi = ImageConversions.imageToBufferedImage(in, scale)

    val outf = new File(fname)
    ImageIO.write(bi, ext, outf)

    val percent = ratio * 100.0
    val fnameScaled = fnamePrefix + "-scaled." + ext
    val cmd = "convert " + fname + " -resize " + percent + "% " + fnameScaled

    val cmdResult = cmd !!

    val scaledFile = new File(fnameScaled)
    if (!scaledFile.exists()) {
      Thread.sleep(5)
    }
    require(scaledFile.exists(), "Could not find scaled file " + fnameScaled)

    val output = ImageConversions.bufferedImageToWrapper(ImageIO.read(new FileInputStream(scaledFile)))

    val a = Seq("rm", "-f", fname, fnameScaled) !!

    output
  }
}
