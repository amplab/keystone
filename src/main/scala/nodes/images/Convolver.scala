package nodes.images

import breeze.linalg._
import nodes.learning.ZCAWhitener
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import pipelines._
import utils.{ChannelMajorArrayVectorizedImage, ImageMetadata, _}
import workflow.Transformer

/**
 * Convolves images with a bank of convolution filters. Convolution filters must be square.
 * Used for using the same label for all patches from an image.
 * TODO: Look into using Breeze's convolve
 *
 * @param filters Bank of convolution filters to apply - each filter is an array in row-major order.
 * @param imgWidth Width of images in pixels.
 * @param imgHeight Height of images in pixels.
 */
class Convolver(
    filters: DenseMatrix[Double],
    imgWidth: Int,
    imgHeight: Int,
    imgChannels: Int,
    whitener: Option[ZCAWhitener] = None,
    normalizePatches: Boolean = true,
    varConstant: Double = 10.0)
  extends Transformer[Image, Image] {

  val convSize = math.sqrt(filters.cols/imgChannels).toInt
  val convolutions = filters.t

  val resWidth = imgWidth - convSize + 1
  val resHeight = imgHeight - convSize + 1

  override def apply(in: RDD[Image]): RDD[Image] = {
    in.mapPartitions(Convolver.convolvePartitions(_, resWidth, resHeight, imgChannels, convSize,
      normalizePatches, whitener, convolutions, varConstant))
  }

  def apply(in: Image): Image = {
    var patchMat = new DenseMatrix[Double](resWidth*resHeight, convSize*convSize*imgChannels)
    Convolver.convolve(in, patchMat, resWidth, resHeight,
      imgChannels, convSize, normalizePatches, whitener, convolutions)
  }
}

object Convolver {
  /**
    * User-friendly constructor interface for the Conovler.
    *
    * @param filters An array of images with which we convolve each input image. These images should *not* be pre-whitened.
    * @param imgInfo Metadata of a typical image we will be convolving. All images must have the same size/shape.
    * @param whitener Whitener to be applied to both the input images and the filters before convolving.
    * @param normalizePatches Should the patches be normalized before convolution?
    * @param varConstant Constant to be used in scaling.
    * @param flipFilters Should the filters be flipped before convolution is applied (used for comparability to MATLAB's
    *                    convnd function.)
    */
  def apply(filters: Array[Image],
           imgInfo: ImageMetadata,
           whitener: Option[ZCAWhitener] = None,
           normalizePatches: Boolean = true,
           varConstant: Double = 10.0,
           flipFilters: Boolean = false) = {

    //If we are told to flip the filters, invert their indexes.
    val filterImages = if (flipFilters) {
      filters.map(flipImage)
    } else filters

    //Pack the filter array into a dense matrix of the right format.
    val packedFilters = packFilters(filterImages)

    //If the whitener is not empty, construct a new one:
    val whitenedFilterMat = whitener match {
      case Some(x) => x.apply(packedFilters) * x.whitener.t
      case None => packedFilters
    }

    new Convolver(
      whitenedFilterMat,
      imgInfo.xDim,
      imgInfo.yDim,
      imgInfo.numChannels,
      whitener,
      normalizePatches,
      varConstant)
  }

  /**
    * Flip the image such that flipImage(im)(x,y,z) = im(im.metadata.xDim-x,im.metadata.yDim-y,im.metadata.numChannels-z)
    * for all valid (x,y,z).
    *
    * @param filt An input image.
    * @return A flipped image.
    */
  def flipImage(filt: Image): Image = {
    val size = filt.metadata.xDim*filt.metadata.yDim*filt.metadata.numChannels
    val res = new ChannelMajorArrayVectorizedImage(Array.fill[Double](size)(0.0), filt.metadata)

    for (
      x <- 0 until filt.metadata.xDim;
      y <- 0 until filt.metadata.yDim;
      c <- 0 until filt.metadata.numChannels
    ) {
      res.put(filt.metadata.xDim - x - 1, filt.metadata.yDim - y - 1, filt.metadata.numChannels - c - 1, filt.get(x,y,c))
    }

    res
  }

  /**
    * Given an array of filters, packs the filters into a DenseMatrix[Double] which has the following form:
    * for a row i, column c+y*numChannels+x*numChannels*yDim corresponds to the pixel value at (x,y,c) in image i of
    * the filters array.
    *
    * @param filters Array of filters.
    * @return DenseMatrix of filters, as described above.
    */
  def packFilters(filters: Array[Image]): DenseMatrix[Double] = {
    val (xDim, yDim, numChannels) = (filters(0).metadata.xDim, filters(0).metadata.yDim, filters(0).metadata.numChannels)
    val filterSize = xDim*yDim*numChannels
    val res = DenseMatrix.zeros[Double](filters.length, filterSize)
    println(s"${res.rows}, ${res.cols}")

    println(s"${filters.length}, $xDim,$yDim,$numChannels")

    var i,x,y,c = 0
    while(i < filters.length) {
      x = 0
      while(x < xDim) {
        y = 0
        while(y < yDim) {
          c = 0
          while (c < numChannels) {
            val rc = c + x*numChannels + y*numChannels*xDim
            res(i, rc) = filters(i).get(x,y,c)

            c+=1
          }
          y+=1
        }
        x+=1
      }
      i+=1
    }

    res
  }


  def convolve(img: Image,
      patchMat: DenseMatrix[Double],
      resWidth: Int,
      resHeight: Int,
      imgChannels: Int,
      convSize: Int,
      normalizePatches: Boolean,
      whitener: Option[ZCAWhitener],
      convolutions: DenseMatrix[Double],
      varConstant: Double = 10.0): Image = {

    val imgMat = makePatches(img, patchMat, resWidth, resHeight, imgChannels, convSize,
      normalizePatches, whitener, varConstant)

    val convRes: DenseMatrix[Double] = imgMat * convolutions

    val res = new ChannelMajorArrayVectorizedImage(
      new Array[Double](resWidth*resHeight*convolutions.cols),
      ImageMetadata(resWidth, resHeight, convolutions.cols))

    // Now pack the convolved features into the result.
    var x, y, chan = 0
    chan = 0
    while (chan < convolutions.cols) {
      y = 0
      while (y < resHeight) {
        x = 0
        while (x < resWidth) {
          res.put(x, y, chan, convRes(x + y * resWidth, chan))
          x += 1
        }
        y += 1
      }
      chan += 1
    }


    res
  }

  /**
   * This function takes an image and generates a matrix of all of its patches. Patches are expected to have indexes
   * of the form: c + x*numChannels + y*numChannels*xDim
   *
   * @param img
   * @return
   */
  def makePatches(img: Image,
      patchMat: DenseMatrix[Double],
      resWidth: Int,
      resHeight: Int,
      imgChannels: Int,
      convSize: Int,
      normalizePatches: Boolean,
      whitener: Option[ZCAWhitener],
      varConstant: Double): DenseMatrix[Double] = {
    var x,y,chan,pox,poy,py,px = 0

    poy = 0
    while (poy < convSize) {
      pox = 0
      while (pox < convSize) {
        y = 0
        while (y < resHeight) {
          x = 0
          while (x < resWidth) {
            chan = 0
            while (chan < imgChannels) {
              px = chan + pox*imgChannels + poy*imgChannels*convSize
              py = x + y*resWidth

              patchMat(py, px) = img.get(x+pox, y+poy, chan)

              chan+=1
            }
            x+=1
          }
          y+=1
        }
        pox+=1
      }
      poy+=1
    }

    val patchMatN = if(normalizePatches) Stats.normalizeRows(patchMat, varConstant) else patchMat

    val res = whitener match {
      case None => patchMatN
      case Some(whiteness) => patchMatN(*, ::) - whiteness.means
    }

    res
  }

  def convolvePartitions(
      imgs: Iterator[Image],
      resWidth: Int,
      resHeight: Int,
      imgChannels: Int,
      convSize: Int,
      normalizePatches: Boolean,
      whitener: Option[ZCAWhitener],
      convolutions: DenseMatrix[Double],
      varConstant: Double): Iterator[Image] = {

    var patchMat = new DenseMatrix[Double](resWidth*resHeight, convSize*convSize*imgChannels)
    imgs.map(convolve(_, patchMat, resWidth, resHeight, imgChannels, convSize, normalizePatches,
      whitener, convolutions, varConstant))

  }
}
