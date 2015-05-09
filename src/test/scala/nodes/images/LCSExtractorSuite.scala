package nodes.images

import breeze.linalg._
import org.scalatest.FunSuite

import pipelines.Logging
import utils.{ImageUtils, Stats, TestUtils}

class LCSExtractorSuite extends FunSuite with Logging {
  test("Load an Image and compute LCS Features") {
    val testImage = TestUtils.loadTestImage("images/gantrycrane.png")

    val lf = new LCSExtractor(stride=4, subPatchSize=6, strideStart=16)
    val lcsDescriptors = convert(lf.apply(testImage), Double)

    val firstKeyPointSum = sum(lcsDescriptors(0, ::))
    val fullFeatureSum = sum(lcsDescriptors)

    // Values found from running matlab code on same input file.
    val matlabFirstKeyPointSum = 3.786557667540610e+03
    val matlabFullFeatureSum = 3.171963632855949e+07

    assert(
      Stats.aboutEq((firstKeyPointSum - matlabFirstKeyPointSum)/matlabFirstKeyPointSum, 0, 1e-8),
      "First keypoint sum must match for LCS")
    assert(Stats.aboutEq((fullFeatureSum - matlabFullFeatureSum)/matlabFullFeatureSum, 0, 1e-8),
      "Sum of LCS must match expected sum")
  }
}
