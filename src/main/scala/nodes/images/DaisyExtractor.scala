package nodes.images

import breeze.linalg._

import pipelines.Transformer
import utils.Image
import utils.ImageUtils

/**
 * Computes DAISY feature descriptors as specified in [1]. Returns the
 * features at a regular spaced grid.
 * 
 * [1] Engin Tola, Vincent Lepetit, and Pascal Fua, "DAISY: An Efficient
 * Dense Descriptor Applied to Wide-Baseline Stereo." IEEE Transactions on
 * Pattern Analysis and Machine Intelligence, Vol. 32, No. 5, 2010.
 * 
 * Based on code by Ben Recht <brecht@cs.berkeley.edu>
 *
 * @param daisyT Number of histograms at a single layer
 * @param daisyQ Number of convolved orientations layers with different sigmas
 * @param daisyR Distance from the center pixel to the outer most grid point
 * @param daisyH Number of bins in the histogram
 * @param pixelBorder Starting offset for keypoints
 * @param stride Stride for keypoints on the grid
 * @param patchSize Size of neighborhood to consider for constructing patches
 */
class DaisyExtractor(
    val daisyT: Int = 8,
    val daisyQ: Int = 3,
    val daisyR: Int = 7,
    val daisyH: Int = 8,
    val pixelBorder: Int = 16,
    val stride: Int = 4,
    val patchSize: Int = 24)
  extends Transformer[Image, DenseMatrix[Float]] {

  // Set a histogram to zero if its size is below this number
  val featureThreshold = 1e-8

  // Where to zero out Gaussian blurs
  val convThreshold = 1e-6

  val daisyFeatureSize = daisyH * (daisyT * daisyQ + 1)
  val filter1 = Array(1.0, 0.0, -1.0)
  val filter2 = Array(1.0, 2.0, 1.0)

  // Given the constants, compute the daisy sigma parameters.
  val daisySigmaSq = (0 to daisyQ).map(n => math.pow(daisyR.toDouble * n/(2 * daisyQ), 2))
  val daisySigmaSqDiff = daisySigmaSq.zip(daisySigmaSq.drop(1)).map { case (a, b) => b - a }

  // Compute blurs.
  val tDaisy = daisySigmaSqDiff.map { t =>
    math.ceil(math.sqrt(
      -2 * t * math.log(convThreshold) - t * math.log(2 * math.Pi * t))).toInt
  }

  val g = tDaisy.zip(0 until daisyQ).map {
    case (t, q) => (-t to t).map(n =>
      math.exp(-(math.pow(n.toDouble, 2)/ (2 * daisySigmaSqDiff(q)) )) /
        math.sqrt(2 * math.Pi * daisySigmaSqDiff(q))
    ).toArray
  }.toArray

  /**
   * Gets a histogram from the daisyLayers.
   * @param daisyLayers The daisy features object. Has Q layers and H blurs per layer.
   * @param x The x-coord of the keypoint.
   * @param y The y-coord of the keypoint.
   * @param level The layer we're on.
   * @param angleCount The angle we're on.
   * @return Histogram (H-vector) of a given key-point/angle/layer.
   */
  private def getHist(
      daisyLayers: Array[Array[Image]], 
      x: Int,
      y: Int,
      level: Int,
      angleCount: Int): Array[Double] = {
    // Current radius
    val curRad = daisyR * (1 + level.toDouble) / daisyQ
    // Current angle
    val curTheta = 2 * math.Pi * (angleCount-1) / daisyT

    // Compute the center coordinates for this keypoint/level/angle
    val lookupStartX = x + math.round(curRad * math.sin(curTheta)).toInt
    val lookupStartY = y + math.round(curRad * math.cos(curTheta)).toInt

    (0 until daisyH).map(daisyLayers(level)(_).get(lookupStartX, lookupStartY, 0)).toArray
  }

  /**
   * Gets a histogram for the center daisy point.
   * @param daisyLayers The daisy features object. Has Q layers and H blurs per layer.
   * @param x The x-coord of the keypoint.
   * @param y The y-coord of the keypoint.
   * @return Histogram (H-vector) of a given key-point/angle/layer.
   */
  private def getCenterHist(daisyLayers: Array[Array[Image]], x: Int, y: Int): Array[Double] = {
    (0 until daisyH).map(daisyLayers(0)(_).get(x, y, 0)).toArray
  }

  /**
   * Computes daisy features of an image.
   * @param in Image
   * @return Daisy features.
   */
  def apply(in: Image): DenseMatrix[Float] = {
    // Compute image gradients.
    val ix = ImageUtils.conv2D(in, filter1, filter2)
    val iy = ImageUtils.conv2D(in, filter2, filter1)

    // Compute Daisy Blur Layers.
    val daisyLayers = Array.ofDim[Image](daisyQ, daisyH)

    var angleCount, l = 0
    while (angleCount < daisyH) {
      // Compute the daisy layer for layer 0.
      val daisyAngle = 2 * math.Pi * angleCount/daisyH
      val newImage = ImageUtils.mapPixels(
                       ImageUtils.pixelCombine(
                         ImageUtils.mapPixels(ix, math.cos(daisyAngle) * _),
                         ImageUtils.mapPixels(iy, math.sin(daisyAngle) * _)),
                       math.max(_, 0.0))

      daisyLayers(0)(angleCount) = ImageUtils.conv2D(newImage, g(0), g(0))

      // Compute the daisy layer for the upper layers.
      l = 1
      while (l < daisyQ) {
        daisyLayers(l)(angleCount) = ImageUtils.conv2D(daisyLayers(l-1)(angleCount), g(l), g(l))
        l += 1
      }
      angleCount += 1
    }

    // Pack grams into image.
    val keyPointXs =
      (pixelBorder to (in.metadata.xDim - pixelBorder - 1) by stride).zipWithIndex.map {
        case (a,b) => (b,a)
      }.toMap
    val keyPointYs =
      (pixelBorder to (in.metadata.yDim - pixelBorder - 1) by stride).zipWithIndex.map {
        case (a,b) => (b,a)
      }.toMap

    val resultHeight = keyPointXs.size
    val resultWidth = keyPointYs.size

    val out = new DenseMatrix[Float](resultHeight * resultWidth, daisyFeatureSize)

    // Compute and normalize histograms for each key point.
    var x, y, off = 0
    l = 0
    angleCount = 0

    while(x < resultHeight) {
      y = 0
      while (y < resultWidth) {
        // Get the histogram for the center.
        val pix = normalize(getCenterHist(daisyLayers, keyPointXs(x), keyPointYs(y)))
        off = 0

        angleCount = 0
        while (off < daisyH) {
          out(x * resultWidth + y, angleCount * daisyQ * daisyH + off) = pix(off).toFloat
          off += 1
        }

        l = 0
        while (l < daisyQ) {
          angleCount = 0
          while (angleCount < daisyT) {
            off = 0
            val pix = normalize(getHist(daisyLayers, keyPointXs(x), keyPointYs(y), l, angleCount))
            while (off < daisyH) {
              out(x * resultWidth + y, daisyH + angleCount * daisyQ * daisyH + l * daisyH + off) = 
                pix(off).toFloat
              off += 1
            }
            angleCount += 1
          }
          l += 1
        }

        y += 1
      }
      x += 1
    }
    out
  }

  def normalize(x: Array[Double]) = {
    var i = 0
    var sumSq = 0.0
    while (i < x.length) {
      sumSq += (x(i) * x(i))
      i = i + 1
    }
    val norm = math.sqrt(sumSq)

    val out = new Array[Double](x.length)
    if (norm > featureThreshold) {
      i = 0
      while (i < x.length) {
        out(i) = x(i) / norm
        i = i + 1
      }
    }
    out
  }
}
