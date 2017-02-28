package keystoneml.nodes.images

import breeze.linalg._
import keystoneml.nodes.images.external.SIFTExtractor
import org.scalatest.FunSuite

import keystoneml.pipelines.Logging
import keystoneml.utils.{ImageUtils, Stats, TestUtils}

class DaisyExtractorSuite extends FunSuite with Logging {
  test("Load an Image and compute Daisy Features") {
    val testImage = TestUtils.loadTestImage("images/gantrycrane.png")
    val grayImage = ImageUtils.toGrayScale(testImage)

    val df = new DaisyExtractor()
    val daisyDescriptors = convert(df.apply(grayImage), Double)

    val firstKeyPointSum = sum(daisyDescriptors(::, 0))
    val fullFeatureSum = sum(daisyDescriptors)

    // Values found from running matlab code on same input file.
    val matlabFirstKeyPointSum = 55.127217737738533
    val matlabFullFeatureSum = 3.240635661296463E5

    // TODO: This should be at most 1e-8 as we are using Floats. But its 1e-5, 1e-7 right now ?
    assert(Stats.aboutEq(
      (firstKeyPointSum - matlabFirstKeyPointSum)/matlabFirstKeyPointSum, 0, 1e-5),
      "First keypoint sum must match for Daisy")
    assert(Stats.aboutEq((fullFeatureSum - matlabFullFeatureSum)/matlabFullFeatureSum, 0, 1e-7),
      "Sum of Daisys must match expected sum")
  }

  test("Daisy and SIFT extractors should have same row/column ordering.") {
    val testImage = TestUtils.loadTestImage("images/gantrycrane.png")
    val grayImage = ImageUtils.toGrayScale(testImage)

    val df = new DaisyExtractor()
    val daisyDescriptors = convert(df.apply(grayImage), Double)

    val se = SIFTExtractor(scaleStep = 2)
    val siftDescriptors = se.apply(grayImage)

    assert(daisyDescriptors.rows == df.daisyFeatureSize && siftDescriptors.rows == se.descriptorSize)

  }
}
