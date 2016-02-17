package utils.images

import org.scalatest.FunSuite
import pipelines.Logging
import utils.VectorizedImage
import utils.TestUtils._

class ImageSuite extends FunSuite with Logging {
  test("Vectorized Image Coordinates Should be Correct") {
    val (x,y,z) = (100,100,3)

    val images = Array[VectorizedImage](
      genChannelMajorArrayVectorizedImage(x,y,z),
      genColumnMajorArrayVectorizedImage(x,y,z),
      genRowMajorArrayVectorizedImage(x,y,z),
      genRowColumnMajorByteArrayVectorizedImage(x,y,z)
    )

    for (
      img <- images;
      idx <- 0 until x*y*z
    ) {
      val coord = img.vectorToImageCoords(idx)
      assert(img.imageToVectorCoords(coord.x,coord.y,coord.channelIdx) == idx,
        s"imageToVectorCoords(vectorToImageCoords(idx)) should be equivalent to identity(idx) for img $img")
    }

    for (
      img <- images;
      xi <- 0 until x;
      yi <- 0 until y;
      zi <- 0 until z
    ) {
      val coord = img.vectorToImageCoords(img.imageToVectorCoords(xi,yi,zi))
      assert((coord.x, coord.y, coord.channelIdx) == (xi,yi,zi),
        s"vectorToImageCoords(imageToVectorCoords(x,y,z)) should be equivalent to identity(x,y,z) for img $img")
    }
  }
}