package nodes.utils.external

import breeze.linalg._
import breeze.numerics._
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
    assert(Stats.aboutEq(result.toArray.sum, 8.6163289E7), "SUM of SIFTs must match the expected norm.")

  }

  test("Load SIFT Descriptors and compute Fisher Vector Features") {

    val siftDescriptor = MatrixUtils.loadCSVFile(TestUtils.getTestResourceURI("images/feats.csv").toString)
    //val gmm =

  }
}