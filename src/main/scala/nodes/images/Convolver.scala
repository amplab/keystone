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
 *
 * @param filters Bank of convolution filters to apply - each filter is an array in row-major order.
 * @param imgWidth Width of images in pixels.
 * @param imgHeight Height of images in pixels.
 * @param imgChannels Number of channels in input images.
 * @param whitener An optional whitening matrix to apply to the image patches.
 * @param varConstant
 */
class Convolver(
    filters: DenseMatrix[Double],
    imgWidth: Int,
    imgHeight: Int,
    imgChannels: Int,
    whitener: Option[ZCAWhitener] = None,
    normalizePatches: Boolean = true,
    varConstant: Double = 10.0,
    patchStride: Int = 1)
  extends Transformer[Image, Image] {

  val convSize = math.sqrt(filters.cols/imgChannels).toInt
  val convolutions = filters.t

  /* This is the size of the spatial grid over
   * which convolutions are performed
   * Note this corresponds to "valid"
   * convolution mode in scipy/numpy/theano
   */

  val resWidth = (imgWidth - convSize + 1)
  val resHeight = (imgHeight - convSize + 1)

  /* This corresponds to the size of the resulting
   * spatial grid AFTER convolution is performed
   * (this includes patchStride)
   */
  val outWidth = math.ceil(resWidth/patchStride.toFloat).toInt
  val outHeight = math.ceil(resHeight/patchStride.toFloat).toInt

  override def apply(in: RDD[Image]): RDD[Image] = {
    in.mapPartitions(Convolver.convolvePartitions(_, resWidth, resHeight, outWidth, outHeight, imgChannels, convSize,
      normalizePatches, whitener, convolutions, varConstant, patchStride))
  }

  def apply(in: Image): Image = {

    var patchMat = new DenseMatrix[Double](outWidth*outHeight, convSize*convSize*imgChannels)
    Convolver.convolve(in, patchMat, resWidth, resHeight, outWidth, outHeight,
      imgChannels, convSize, normalizePatches, whitener, convolutions, varConstant, patchStride)
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
           flipFilters: Boolean = false,
           patchStride: Int = 1) = {

    //If we are told to flip the filters, invert their indexes.
    val filterImages = if (flipFilters) {
      filters.map(ImageUtils.flipImage)
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
      varConstant,
      patchStride)
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
      outWidth: Int,
      outHeight: Int,
      imgChannels: Int,
      convSize: Int,
      normalizePatches: Boolean,
      whitener: Option[ZCAWhitener],
      convolutions: DenseMatrix[Double],
      varConstant: Double = 10.0,
      patchStride: Int = 1): Image = {

    val imgMat = makePatches(img, patchMat, resWidth, resHeight, outWidth, outHeight, imgChannels, convSize,
      normalizePatches, whitener, varConstant, patchStride)

    val convRes: DenseMatrix[Double] = imgMat * convolutions

    val res = new RowMajorArrayVectorizedImage(
      convRes.toArray,
      ImageMetadata(outWidth, outHeight, convolutions.cols))

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
      outWidth: Int,
      outHeight: Int,
      imgChannels: Int,
      convSize: Int,
      normalizePatches: Boolean,
      whitener: Option[ZCAWhitener],
      varConstant: Double,
      patchStride: Int): DenseMatrix[Double] = {
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
              py = math.ceil((x/patchStride + y*resWidth/(patchStride*patchStride))).toInt
              patchMat(py, px) = img.get(x+pox, y+poy, chan)
              chan+=1
            }
            x += patchStride
          }
          y += patchStride
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
      outWidth: Int,
      outHeight: Int,
      imgChannels: Int,
      convSize: Int,
      normalizePatches: Boolean,
      whitener: Option[ZCAWhitener],
      convolutions: DenseMatrix[Double],
      varConstant: Double,
      patchStride: Int): Iterator[Image] = {

    var patchMat = new DenseMatrix[Double](outWidth*outHeight, convSize*convSize*imgChannels)
    imgs.map(convolve(_, patchMat, resWidth, resHeight, outWidth, outHeight, imgChannels, convSize, normalizePatches,
      whitener, convolutions, varConstant, patchStride))

  }
}
