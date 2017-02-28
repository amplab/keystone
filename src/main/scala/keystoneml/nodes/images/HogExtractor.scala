/**
 * -------------------------------------------------------
 * Copyright (C) 2014 Henry Milner
 * Copyright (C) 2011-2012 Ross Girshick
 * Copyright (C) 2008, 2009, 2010 Pedro Felzenszwalb, Ross Girshick
 * Copyright (C) 2007 Pedro Felzenszwalb, Deva Ramanan
 *
 * This file is part of the voc-releaseX code
 * (http://people.cs.uchicago.edu/~rbg/latent/)
 * and is available under the terms of an MIT-like license
 * provided in COPYING. Please retain this notice and
 * COPYING if you use this file (or a portion of it) in
 * your project.
 * -------------------------------------------------------
 */

package keystoneml.nodes.images

import breeze.linalg._

import keystoneml.workflow.Transformer
import keystoneml.utils.ChannelMajorArrayVectorizedImage
import keystoneml.utils.Image
import keystoneml.utils.ImageUtils

/**
 * Histogram of Gradients (HoG)
 * 
 * Translated by Henry Milner from C code originally written by Ross Girshick.
 * The original code is available at:
 * https://github.com/rbgirshick/voc-dpm/blob/master/features/features.cc
 */
class HogExtractor(binSize: Int) extends Transformer[Image, DenseMatrix[Float]] {

  // small value, used to avoid division by zero
  private val EPSILON = 0.0001

  // unit vectors used to compute gradient orientation
  private val uu = Array(
    1.0000,
    0.9397,
    0.7660,
    0.500,
    0.1736,
    -0.1736,
    -0.5000,
    -0.7660,
    -0.9397)

  private val vv = Array(
    0.0000,
    0.3420,
    0.6428,
    0.8660,
    0.9848,
    0.9848,
    0.8660,
    0.6428,
    0.3420)

  private val numOrientations = uu.length

  def apply(image: Image): DenseMatrix[Float] = {
    val numXCells = math.round(image.metadata.xDim.toDouble / binSize).toInt
    val numYCells = math.round(image.metadata.yDim.toDouble / binSize).toInt
    val numChannels = image.metadata.numChannels

    val hist = computeHist(image, numXCells, numYCells, numChannels, binSize)
    val norm = computeNormsFromHist(hist, numXCells, numYCells)
    computeFeaturesFromHist(hist, norm, numXCells, numYCells)
  }

  def computeHist(
      image: Image,
      numXCells: Int,
      numYCells: Int,
      numChannels: Int,
      binSize: Int): Array[Float] = {
    // A flat array conceptually indexed by an (x, y, orientationIdx) triple.  To
    // reference the element at (x, y, o) use hist(x + y*numXCells + o*numYCells*numXCells).
    val hist: Array[Float] = Array.ofDim(numXCells * numYCells * 18)

    val numVisibleXPixels = numXCells * binSize
    val numVisibleYPixels = numYCells * binSize

    var x = 1
    while (x < numVisibleXPixels - 1) {
      var y = 1
      while (y < numVisibleYPixels - 1) {

        // First we compute the channel with the highest-magnitude gradient,
        // and throw away the other channels for this pixel.
        var highestMagnitudeChannel = -1
        var bestChannelMagnitudeSquared = Double.NegativeInfinity
        var bestChannelDx = Double.NegativeInfinity
        var bestChannelDy = Double.NegativeInfinity
        var channelIdx = 2

        while (channelIdx >= 0) {
          val dx = image.get(x + 1, y, channelIdx) - image.get(x - 1, y, channelIdx)
          val dy = image.get(x, y + 1, channelIdx) - image.get(x, y - 1, channelIdx)
          val magnitudeSquared = dx*dx + dy*dy
          if (magnitudeSquared > bestChannelMagnitudeSquared) {
            highestMagnitudeChannel = channelIdx
            bestChannelMagnitudeSquared = magnitudeSquared
            bestChannelDx = dx
            bestChannelDy = dy
          }
          channelIdx -= 1
        }
        val dx = bestChannelDx
        val dy = bestChannelDy
        val magnitude = math.sqrt(bestChannelMagnitudeSquared)

        // We snap to one of 18 orientations.
        var bestOrientationDot = 0.0 //Double.NegativeInfinity
        var bestOrientationIdx = 0
        var orientationIdx = 0
        while (orientationIdx < numOrientations) {
          val dot = uu(orientationIdx)*dy + vv(orientationIdx)*dx
          if (dot > bestOrientationDot) {
            bestOrientationIdx = orientationIdx
            bestOrientationDot = dot
          } else if (-dot > bestOrientationDot) {
            bestOrientationIdx = orientationIdx + numOrientations
            bestOrientationDot = -dot
          }
          orientationIdx += 1
        }

        // We add to 4 histograms around the pixel (x,y) using bilinear interpolation
        // TODO: Clean this.  It's just rounding some stuff to the nearest bin,
        // but it looks way more mysterious than it should.
        val yp = (y + 0.5)/binSize - 0.5
        val xp = (x + 0.5)/binSize - 0.5
        val iyp = math.floor(yp).toInt
        val ixp = math.floor(xp).toInt

        val vy0 = yp - iyp
        val vx0 = xp - ixp
        val vy1 = 1.0 - vy0
        val vx1 = 1.0 - vx0

        if (iyp >= 0 && ixp >= 0) {
          hist(ixp + iyp*numXCells + bestOrientationIdx*numXCells*numYCells) +=
            (vy1*vx1*magnitude).toFloat
        }
        if (iyp + 1 < numYCells && ixp >= 0) {
          hist(ixp + (iyp + 1)*numXCells + bestOrientationIdx*numXCells*numYCells) +=
            (vy0*vx1*magnitude).toFloat
        }
        if (iyp >= 0 && ixp + 1 < numXCells) {
          hist((ixp + 1) + iyp*numXCells + bestOrientationIdx*numXCells*numYCells) +=
            (vy1*vx0*magnitude).toFloat
        }
        if (iyp + 1 < numYCells && ixp + 1 < numXCells) {
          hist((ixp + 1) + (iyp + 1)*numXCells + bestOrientationIdx*numXCells*numYCells) +=
            (vy0*vx0*magnitude).toFloat
        }
        y += 1
      }
      x += 1
    }
    hist
  }

  def computeNormsFromHist(hist: Array[Float], numXCells: Int, numYCells: Int): Array[Float] = {
    // A flat array conceptually indexed by an (x, y) pair.  To reference
    // the element at (x, y) use hist(x + y*numXCells).
    val norm: Array[Float] = Array.ofDim(numXCells * numYCells)

    // Compute energy in each block by summing over orientations.
    // Values for opposite orientations are combined.
    // norm(x, y) = sum_{o < 9} (hist(x, y, o) + hist(x, y, o+9))^2
    var o = 0
    while (o < 9) {
      val oppositeO = o + 9
      var y = 0
      while (y < numYCells) {
        var x = 0
        while (x < numXCells) {
          val histValueO = hist(x + y*numXCells + o*numXCells*numYCells)
          val histValueOppositeO = hist(x + y*numXCells + oppositeO*numXCells*numYCells)
          norm(x + y*numXCells) += 
            (histValueO + histValueOppositeO) * (histValueO + histValueOppositeO)
          x += 1
        }
        y += 1
      }
      o += 1
    }
    norm
  }

  private def computeFeaturesFromHist(
      hist: Array[Float],
      norm: Array[Float],
      numXCells: Int,
      numYCells: Int): DenseMatrix[Float] = {
    val numXCellsWithFeatures = math.max(numXCells-2, 0)
    val numYCellsWithFeatures = math.max(numYCells-2, 0)
    val numFeatures = 27 + 4 + 1
    // Indexed by (x + y*numXCellsWithFeatures, featureIdx)
    val features = new DenseMatrix[Float](
      numXCellsWithFeatures * numYCellsWithFeatures, numFeatures)

    var x = 0
    while (x < numXCellsWithFeatures) {
      var y = 0
      while (y < numYCellsWithFeatures) {
        val featureDestinationStartingIdx = y + x*numYCellsWithFeatures

        val n1NormOffset = (y + 1)*numXCells + (x + 1)
        val n1 = 1.0 / math.sqrt(norm(n1NormOffset) + norm(n1NormOffset + 1) +
          norm(n1NormOffset + numXCells) + norm(n1NormOffset + numXCells + 1) + EPSILON)

        val n2NormOffset = (y + 1)*numXCells + x
        val n2 = 1.0 / math.sqrt(norm(n2NormOffset) + norm(n2NormOffset + 1) +
          norm(n2NormOffset + numXCells) + norm(n2NormOffset + numXCells + 1) + EPSILON)

        val n3NormOffset = y*numXCells + (x + 1)
        val n3 = 1.0 / math.sqrt(norm(n3NormOffset) + norm(n3NormOffset + 1) +
          norm(n3NormOffset + numXCells) + norm(n3NormOffset + numXCells + 1) + EPSILON)

        val n4NormOffset = y*numXCells + x
        val n4 = 1.0 / math.sqrt(norm(n4NormOffset) + norm(n4NormOffset + 1) +
          norm(n4NormOffset + numXCells) + norm(n4NormOffset + numXCells + 1) + EPSILON)

        var t1 = 0.0
        var t2 = 0.0
        var t3 = 0.0
        var t4 = 0.0

        // Contrast-sensitive features
        // TODO: Clean up the indexing here; it uses pointer-incrementing style
        // rather than calculating indices explicitly, which would be less risky,
        // clearer, and probably not significantly less performant.
        var histOffset0 = (y + 1)*numXCells + (x + 1)
        var o0 = 0
        var featureOffset0 = 0
        while (o0 < 18) {
          val h1 = math.min(hist(histOffset0) * n1, 0.2)
          val h2 = math.min(hist(histOffset0) * n2, 0.2)
          val h3 = math.min(hist(histOffset0) * n3, 0.2)
          val h4 = math.min(hist(histOffset0) * n4, 0.2)
          features(featureDestinationStartingIdx, featureOffset0) = 
            (0.5 * (h1 + h2 + h3 + h4)).toFloat
          t1 += h1
          t2 += h2
          t3 += h3
          t4 += h4
          featureOffset0 += 1
          histOffset0 += (numXCells * numYCells)
          o0 += 1
        }

        // contrast-insensitive features
        var o1 = 0
        var histOffset1 = (y + 1)*numXCells + (x + 1)
        var featureOffset1 = featureOffset0
        while (o1 < 9) {
          val sum = hist(histOffset1) + hist(histOffset1 + 9*numXCells*numYCells)
          val h1 = math.min(sum * n1, 0.2)
          val h2 = math.min(sum * n2, 0.2)
          val h3 = math.min(sum * n3, 0.2)
          val h4 = math.min(sum * n4, 0.2)
          features(featureDestinationStartingIdx, featureOffset1) =
            (0.5 * (h1 + h2 + h3 + h4)).toFloat
          featureOffset1 += 1
          histOffset1 += (numXCells * numYCells)
          o1 += 1
        }

        // texture features
        var featureOffset2 = featureOffset1
        features(featureDestinationStartingIdx, featureOffset2) = (0.2357 * t1).toFloat
        featureOffset2 += 1
        features(featureDestinationStartingIdx, featureOffset2) = (0.2357 * t2).toFloat
        featureOffset2 += 1
        features(featureDestinationStartingIdx, featureOffset2) = (0.2357 * t3).toFloat
        featureOffset2 += 1
        features(featureDestinationStartingIdx, featureOffset2) = (0.2357 * t4).toFloat

        // truncation feature
        var featureOffset3 = featureOffset2
        featureOffset3 += 1
        features(featureDestinationStartingIdx, featureOffset3) = 0

        y += 1
      }
      x += 1
    }

    features
  }
}
