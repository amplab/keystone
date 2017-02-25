package keystoneml.nodes.images

import breeze.linalg._
import org.scalatest.FunSuite

import keystoneml.pipelines.Logging
import keystoneml.utils.{ImageUtils, Stats, TestUtils}

class HogExtractorSuite extends FunSuite with Logging {
  test("Load an Image and compute Hog Features") {
    val testImage = TestUtils.loadTestImage("images/gantrycrane.png")

    // NOTE: The MATLAB implementation from voc-release5 uses
    // images in double range -- So convert our image by rescaling
    val testImageScaled = ImageUtils.mapPixels(testImage, x => x/255.0)

    val binSize = 50
    val hog = new HogExtractor(binSize)
    val descriptors = hog.apply(testImageScaled)

    val ourSum = sum(descriptors)
    val matlabSum = 59.2162514

    assert(Stats.aboutEq((ourSum - matlabSum) / ourSum, 0, 1e-8),
      "Hog features sum should match")

    // With a smaller bin size
    val hog1 = new HogExtractor(binSize=8)
    val descriptors1 = hog1.apply(testImageScaled)

    val matlabSum1 = 4.5775269e+03
    val ourSum1 = sum(descriptors1)

    // TODO: Figure out why error is a bit higher here ?
    assert(Stats.aboutEq((ourSum1 - matlabSum1) / ourSum1, 0, 1e-4),
      "Hog features sum should match")
  }
}
