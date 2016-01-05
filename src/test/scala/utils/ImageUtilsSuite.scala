package utils

import org.scalatest.FunSuite

class ImageUtilsSuite extends FunSuite {

  test("crop") {

    val imgArr =
      (0 until 4).flatMap { x =>
        (0 until 4).flatMap { y =>
          (0 until 1).map { c =>
            (c + x * 1 + y * 4 * 1).toDouble
          }
        }
      }.toArray

    val image = new ChannelMajorArrayVectorizedImage(imgArr, ImageMetadata(4, 4, 1))

    (0 until 4).foreach { x =>
      println((0 until 4).map { y => image.get(x, y, 0) }.mkString(" "))
    }

  }

}
