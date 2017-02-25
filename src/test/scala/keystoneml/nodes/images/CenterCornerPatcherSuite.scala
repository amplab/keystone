package keystoneml.nodes.images

import org.scalatest.FunSuite
import keystoneml.pipelines.Logging
import keystoneml.utils.{ChannelMajorArrayVectorizedImage, ImageMetadata, TestUtils}

class CenterCornerPatcherSuite extends FunSuite with Logging {

  test("check number and dimension of patches") {
    val image = TestUtils.loadTestImage("images/000012.jpg")
    val xDim = image.metadata.xDim
    val yDim = image.metadata.yDim
    val patchSizeX = xDim / 2 
    val patchSizeY = yDim / 2

    val withFlipPatcher = CenterCornerPatcher(patchSizeX, patchSizeY, true)
    val withFlipPatches = withFlipPatcher.centerCornerPatchImage(image).toSeq

    assert(withFlipPatches.map(_.metadata.xDim).forall(_ == patchSizeX) &&
      withFlipPatches.map(_.metadata.yDim).forall(_ == patchSizeY) &&
      withFlipPatches.map(_.metadata.numChannels).forall(_ == image.metadata.numChannels),
      "All patches must have right dimensions")

    assert(withFlipPatches.size === 10, "Number of patches must match")

    val noFlipPatcher = CenterCornerPatcher(patchSizeX, patchSizeY, false) 
    val noFlipPatches = noFlipPatcher.centerCornerPatchImage(image).toSeq

    assert(noFlipPatches.map(_.metadata.xDim).forall(_ == patchSizeX) &&
      noFlipPatches.map(_.metadata.yDim).forall(_ == patchSizeY) &&
      noFlipPatches.map(_.metadata.numChannels).forall(_ == image.metadata.numChannels),
      "All patches must have right dimensions")

    assert(noFlipPatches.size === 5, "Number of patches must match")
  }

  test("1x1 image patches") {
    val imgArr =
      (0 until 5).flatMap { x =>
        (0 until 5).flatMap { y =>
          (0 until 1).map { c =>
            (c + x * 1 + y * 5 * 1).toDouble
          }
        }
      }.toArray

    val image = new ChannelMajorArrayVectorizedImage(imgArr, ImageMetadata(5, 5, 1))
    val patchSizeX = 1
    val patchSizeY = 1

    val noFlipPatcher = CenterCornerPatcher(patchSizeX, patchSizeY, false)
    val noFlipPatches = noFlipPatcher.centerCornerPatchImage(image).toSeq

    assert(noFlipPatches.length === 5)
    // NOTE(shivaram): This assumes order of patches returned stays the same. 
    assert(noFlipPatches(0).get(0, 0, 0) === 0.0)
    assert(noFlipPatches(1).get(0, 0, 0) === 20.0)
    assert(noFlipPatches(2).get(0, 0, 0) === 4.0)
    assert(noFlipPatches(3).get(0, 0, 0) === 24.0)
    assert(noFlipPatches(4).get(0, 0, 0) === 12.0)
  }
}
