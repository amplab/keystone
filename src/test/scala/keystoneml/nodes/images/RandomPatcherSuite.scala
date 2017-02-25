package keystoneml.nodes.images

import org.scalatest.FunSuite
import keystoneml.pipelines.Logging
import keystoneml.utils.{ChannelMajorArrayVectorizedImage, ImageMetadata, TestUtils}

class RandomPatcherSuite extends FunSuite with Logging {

  test("patch dimensions, number") {
    val image = TestUtils.loadTestImage("images/000012.jpg")
    val xDim = image.metadata.xDim
    val yDim = image.metadata.yDim
    val patchSizeX = xDim / 2 
    val patchSizeY = yDim / 2
    val numPatches = 5

    val patcher = RandomPatcher(numPatches, patchSizeX, patchSizeY)

    val patches = patcher.randomPatchImage(image).toSeq

    assert(patches.map(_.metadata.xDim).forall(_ == patchSizeX) &&
      patches.map(_.metadata.yDim).forall(_ == patchSizeY) &&
      patches.map(_.metadata.numChannels).forall(_ == image.metadata.numChannels),
      "All patches must have right dimensions")

    assert(patches.size === numPatches,
      "Number of patches must match argument passed in")
  }
}
