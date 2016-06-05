package nodes.images.external

import org.scalatest.FunSuite
import pipelines.{Logging, PipelineContext}
import utils.{ImageUtils, TestUtils}

class SIFTExtractorSuite extends FunSuite with Logging {
  test("Test Sift on a single image RDD, scaleStep=1 and scaleStep=0, 0 should have more descriptors") {
    val testImage = TestUtils.loadTestImage("images/000012.jpg")
    val singleImage = ImageUtils.mapPixels(testImage, _/255.0)
    val grayImage = ImageUtils.toGrayScale(singleImage)

    val se1 = SIFTExtractor(scaleStep = 1)
    val res1 = se1(grayImage)

    val se0 = SIFTExtractor(scaleStep = 0)
    val res0 = se0(grayImage)

    logInfo(s"Scale 1 shape is: ${res1.rows}x${res1.cols}")
    logInfo(s"Scale 0 shape is: ${res0.rows}x${res0.cols}")

    assert(res1.cols < res0.cols)

  }

}