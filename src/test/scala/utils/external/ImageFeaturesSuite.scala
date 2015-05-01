package utils.external

import breeze.linalg._
import breeze.numerics._
import nodes.learning.GaussianMixtureModel
import org.scalatest.FunSuite
import pipelines.Logging
import utils.TestUtils._
import utils.{Stats, MatrixUtils, ImageUtils, TestUtils}

class ImageFeaturesSuite extends FunSuite with Logging {
  test("Load an Image and compute SIFT Features") {
    val testImage = TestUtils.loadTestImage("images/000012.jpg")
    val grayImage = ImageUtils.toGrayScale(testImage)

    val extLib = new ImageFeatures

    val stepSize = 4
    val binSize = 6
    val scales = 5
    val descriptorLength = 128

    val rawDescDataShort = extLib.getSIFTs(grayImage.metadata.xDim, grayImage.metadata.yDim,
      stepSize, binSize, scales, grayImage.getSingleChannelAsFloatArray())

    assert(rawDescDataShort.length % descriptorLength == 0, "Resulting SIFTs must be 128-dimensional.")

    val numCols = rawDescDataShort.length/descriptorLength
    val result = new DenseMatrix(descriptorLength, numCols, rawDescDataShort.map(_.toDouble))

    log.info(s"SIFT is ${result.toArray.sum}")
    assert(Stats.aboutEq(result.toArray.sum, 8.6163289E7), "SUM of SIFTs must match the expected sum.")

  }

  test("Load SIFT Descriptors and compute Fisher Vector Features") {

    val siftDescriptor = MatrixUtils.loadCSVFile(TestUtils.getTestResourceFileName("images/feats.csv").toString)

    val gmmMeans = TestUtils.getTestResourceFileName("images/voc_codebook/means.csv")
    val gmmVars = TestUtils.getTestResourceFileName("images/voc_codebook/variances.csv")
    val gmmWeights = TestUtils.getTestResourceFileName("images/voc_codebook/priors")

    val gmm = GaussianMixtureModel.load(gmmMeans, gmmVars, gmmWeights)

    val nCenters = gmm.means.cols
    val nDim = gmm.means.rows

    val extLib = new ImageFeatures

    val fisherVector = extLib.calcAndGetFVs(
      gmm.means.toArray.map(_.toFloat),
      nCenters,
      nDim,
      gmm.variances.toArray.map(_.toFloat),
      gmm.weights.toArray.map(_.toFloat),
      siftDescriptor.toArray.map(_.toFloat))

    log.info(s"Fisher Vector is ${fisherVector.sum}")
    assert(Stats.aboutEq(fisherVector.sum, 40.109097, 1e-4), "SUM of Fisher Vectors must match expected sum.")

  }


}