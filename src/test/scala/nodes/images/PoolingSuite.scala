package nodes.images

import breeze.linalg.{DenseVector, sum}
import nodes._
import org.scalatest.FunSuite
import pipelines.Logging
import utils.{ArrayVectorizedImage, ImageMetadata}

class PoolingSuite extends FunSuite with Logging {

  test("pooling") {
    val imgArr =
      (0 until 4).flatMap { x =>
        (0 until 4).flatMap { y =>
          (0 until 1).map { c =>
            (c + x * 1 + y * 4 * 1).toDouble
          }
        }
      }.toArray

    val image = new ArrayVectorizedImage(imgArr, ImageMetadata(4, 4, 1))
    val pooling = new Pooler(2, 2, x => x, x => x.max)

    val poolImage = pooling(image)

    assert(poolImage.get(0, 0, 0) === 5.0)
    assert(poolImage.get(0, 1, 0) === 7.0)
    assert(poolImage.get(1, 0, 0) === 13.0)
    assert(poolImage.get(1, 1, 0) === 15.0)
  }

  test("pooling odd") {
    val hogImgSize = 14
    val convSizes = List(1, 2, 3, 4, 6, 8)
    convSizes.foreach { convSize =>
      val convResSize = hogImgSize - convSize + 1

      val imgArr =
        (0 until convResSize).flatMap { x =>
          (0 until convResSize).flatMap { y =>
            (0 until 1000).map { c =>
              (c + x * 1 + y * 4 * 1).toDouble
            }
          }
        }.toArray

      val image = new ArrayVectorizedImage(imgArr, ImageMetadata(convResSize, convResSize, 1000))

      val poolSizeReqd = math.ceil(convResSize / 2.0).toInt

      // We want poolSize to be even !!
      val poolSize = (math.ceil(poolSizeReqd / 2.0) * 2).toInt
      // overlap as little as possible
      val poolStride = convResSize - poolSize


      println(s"VALUES: $convSize $convResSize $poolSizeReqd $poolSize $poolStride")

      def summ(x: DenseVector[Double]): Double = sum(x)

      val pooling = new Pooler(poolStride, poolSize, identity, summ)
      val poolImage = pooling(image)
    }
  }
}
