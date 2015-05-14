package utils.external

import breeze.linalg._
import breeze.numerics.abs
import nodes.learning.GaussianMixtureModel
import org.scalatest.FunSuite
import pipelines.Logging
import utils.{ImageUtils, MatrixUtils, Stats, TestUtils}

class VLFeatSuite extends FunSuite with Logging {
  test("Load an Image and compute SIFT Features") {
    val testImage = TestUtils.loadTestImage("images/000012.jpg")
    val singleImage = ImageUtils.mapPixels(testImage, _/255.0)
    val grayImage = ImageUtils.toGrayScale(singleImage)

    val extLib = new VLFeat

    val stepSize = 3
    val binSize = 4
    val scales = 4
    val descriptorLength = 128
    val scaleStep = 0

    val rawDescDataShort = extLib.getSIFTs(grayImage.metadata.xDim, grayImage.metadata.yDim,
      stepSize, binSize, scales, scaleStep, grayImage.getSingleChannelAsFloatArray())

    assert(rawDescDataShort.length % descriptorLength == 0, "Resulting SIFTs must be 128-dimensional.")

    val numCols = rawDescDataShort.length/descriptorLength
    val result = new DenseMatrix(descriptorLength, numCols, rawDescDataShort.map(_.toDouble))

    // Compare with the output of running this image through vl_phow with matlab from the enceval package:
    // featpipem_addpaths;
    // im = im2single(imread('images/000012.jpg'));
    // featextr = featpipem.features.PhowExtractor();
    // featextr.step = 3;
    // [frames feats] = featextr.compute(im);
    // csvwrite('images/feats128.csv', feats)

    val testFeatures = MatrixUtils.loadCSVFile(TestUtils.getTestResourceFileName("images/feats128.csv"))

    val diff = result - testFeatures

    // Because of subtle differences in the way image smoothing works in the VLFeat C library and the VLFeat matlab
    // library (vl_imsmooth_f vs. _vl_imsmooth_f), these two matrices will not be exactly the same.
    // Instead, we check that 99.5% of the matrix entries are off by less than one.
    val absdiff = abs(diff).toDenseVector

    assert(absdiff.findAll(_ > 1.0).length.toDouble < 0.005*absdiff.length,
      "Fewer than 0.05% of entries may be different by more than 1.")
  }
}