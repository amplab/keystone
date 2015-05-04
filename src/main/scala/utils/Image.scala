package utils

import java.awt.image.{BufferedImage, DataBufferByte}
import java.io.InputStream
import javax.imageio.ImageIO

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

  /**
   * Get the pixel value at (x, y, channelIdx).  Channels are indexed as
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

  /**
   * Returns a flat version of the image, represented as a single array.
   * It is indexed as follows: The pixel value for (x, y, channelIdx)
   * is at channelIdx + x*numChannels + y*numChannels*xDim.
   *
   * This implementation works for arbitrary image formats but it is
   * inefficient.
   */
  def toArray: Array[Double] = {
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

  def getSingleChannelAsIntArray(): Array[Int] = {
    if (this.metadata.numChannels > 1) {
      throw new RuntimeException("Cannot call getSingleChannelAsIntArray on an image with more than one channel.")
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

  def getSingleChannelAsFloatArray(): Array[Float] = {
    if (this.metadata.numChannels > 1) {
      throw new RuntimeException("Cannot call getSingleChannelAsFloatArray on an image with more than one channel.")
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


  /**
   * An inefficient implementation of equals().  Subclasses should override
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

  override def toArray = vectorizedImage
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

  override def putInVector(vectorIdx: Int, newVal: Double) = ???
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
    val cidx = channelIdx
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
 */
case class RowColumnMajorArrayVectorizedImage(
                                               vectorizedImage: Array[Double],
                                               override val metadata: ImageMetadata) extends VectorizedImage {
  override def imageToVectorCoords(x: Int, y: Int, channelIdx: Int): Int = {
    val cidx = channelIdx
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

/**
 * Helper trait for implementing Images that wrap vectorized representations
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
