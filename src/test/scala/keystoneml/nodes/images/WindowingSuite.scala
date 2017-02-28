package keystoneml.nodes.images

import org.scalatest.FunSuite
import keystoneml.pipelines.Logging
import keystoneml.utils.{ChannelMajorArrayVectorizedImage, ImageMetadata, TestUtils}

class WindowingSuite extends FunSuite with Logging {

  test("windowing") {
    val image = TestUtils.loadTestImage("images/000012.jpg")
    val stride = 100
    val size = 50

    val windowing = new Windower(stride, size)

    val windows = windowing.getImageWindow(image)

    assert(windows.map(_.metadata.xDim).forall(_ == size) &&
      windows.map(_.metadata.yDim).forall(_ == size),
      "All windows must be 100x100")

    assert(windows.size == (image.metadata.xDim/stride) * (image.metadata.yDim/stride),
      "Must have number of windows matching xDims and yDims given the stride.")
  }

  test("1x1 windowing") {
    val imgArr =
      (0 until 4).flatMap { x =>
        (0 until 4).flatMap { y =>
          (0 until 1).map { c =>
            (c + x * 1 + y * 4 * 1).toDouble
          }
        }
      }.toArray


    val image = new ChannelMajorArrayVectorizedImage(imgArr, ImageMetadata(4, 4, 1))

    val windower = new Windower(1, 1)
    val windowImages = windower.getImageWindow(image)

    assert(windowImages.length === 16)
    assert(windowImages(0).get(0, 0, 0) === 0)
    assert(windowImages(1).get(0, 0, 0) === 1.0)
    assert(windowImages(2).get(0, 0, 0) === 2.0)
    assert(windowImages(3).get(0, 0, 0) === 3.0)
  }

  test("2x2 windowing") {
    val imgArr =
      (0 until 4).flatMap { x =>
        (0 until 4).flatMap { y =>
          (0 until 1).map { c =>
            (c + x * 1 + y * 4 * 1).toDouble
          }
        }
      }.toArray


    val image = new ChannelMajorArrayVectorizedImage(imgArr, ImageMetadata(4, 4, 1))

    val windower = new Windower(2, 2)

    val windowImages = windower.getImageWindow(image)

    assert(windowImages.length === 4)

    assert(windowImages(0).get(0, 0, 0) === 0)
    assert(windowImages(1).get(0, 0, 0) === 2.0)
    assert(windowImages(2).get(0, 0, 0) === 8.0)
    assert(windowImages(3).get(0, 0, 0) === 10.0)
  }

  test("nxn windowing with step=1") {
    val dim = 30
    val imgArr =
      (0 until dim).flatMap { x =>
        (0 until dim).flatMap { y =>
          (0 until 1).map { c =>
            (c + x * 1 + y * 4 * 1 + 10).toDouble
          }
        }
      }.toArray


    val image = new ChannelMajorArrayVectorizedImage(imgArr, ImageMetadata(dim, dim, 1))
    val sizes = List(1, 2, 3, 4, 6, 8)

    sizes.foreach { w =>
      val windower = new Windower(1, w)
      val windowImages = windower.getImageWindow(image)
      assert(windowImages.length === (dim-w+1) * (dim-w+1))
      assert(windowImages.forall(x => !x.toArray.contains(0.0)))
    }
  }
}
