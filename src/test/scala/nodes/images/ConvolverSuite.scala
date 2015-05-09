package nodes.images

import breeze.linalg.{DenseVector, DenseMatrix}
import org.scalatest.FunSuite

import pipelines.{Logging, LocalSparkContext}
import utils._
import org.apache.spark.SparkContext
import utils.ArrayVectorizedImage
import utils.ImageMetadata

class ConvolverSuite extends FunSuite with LocalSparkContext with Logging {

  test("1x1 patches convolutions") {
    val imgWidth = 4
    val imgHeight = 4
    val imgChannels = 3
    val convSize = 1

    val imgArr =
      (0 until imgWidth).flatMap { x =>
        (0 until imgHeight).flatMap { y =>
          (0 until imgChannels).map { c =>
            (c + x * imgChannels + y * imgWidth * imgChannels).toDouble
          }
        }
      }.toArray

    val image = new ArrayVectorizedImage(imgArr, ImageMetadata(imgWidth, imgHeight, imgChannels))

    var conv1 = DenseVector.zeros[Double](convSize * convSize * imgChannels)
    conv1(2) = 1.0

    var conv2 = DenseVector.zeros[Double](convSize * convSize * imgChannels)
    conv2(0) = 0.33
    conv2(1) = 0.33
    conv2(2) = 0.33

    var convBank = MatrixUtils.rowsToMatrix(Array(conv1, conv2))

    val convolver = new Convolver(convBank, imgWidth, imgHeight, imgChannels)

    val poolImage = convolver(image)

    logInfo(s"Image: ${poolImage.toArray.mkString(",")}")
    logInfo(s"Image Dimensions ${poolImage.metadata.xDim} ${poolImage.metadata.yDim} ${poolImage.metadata.numChannels}")

    assert(poolImage.metadata.xDim == image.metadata.xDim - convSize + 1, "Convolved image should have the right xDims.")
    assert(poolImage.metadata.yDim == image.metadata.yDim - convSize + 1, "Convolved image should have the right yDims.")
    assert(poolImage.metadata.numChannels == convBank.rows, "Convolved image should have the right num channels.")
  }

  test("convolutions") {

    val imgWidth = 10
    val imgHeight = 10
    val imgChannels = 3
    val convSize = 3

    val imgArr =
      (0 until imgWidth).flatMap { x =>
        (0 until imgHeight).flatMap { y =>
          (0 until imgChannels).map { c =>
            (c + x * imgChannels + y * imgWidth * imgChannels).toDouble
          }
        }
      }.toArray

    val image = new ArrayVectorizedImage(imgArr, ImageMetadata(imgWidth, imgHeight, imgChannels))

    var conv1 = DenseVector.zeros[Double](convSize * convSize * imgChannels)
    conv1(4) = 1.0

    var conv2 = DenseVector.zeros[Double](convSize * convSize * imgChannels)
    conv2(4) = 0.33
    conv2(4+9) = 0.33
    conv2(4+9+9) = 0.33

    var convBank = MatrixUtils.rowsToMatrix(Array(conv1, conv2))

    val convolver = new Convolver(convBank, imgWidth, imgHeight, imgChannels)

    val poolImage = convolver(image)

    logInfo(s"Image: ${poolImage.toArray.mkString(",")}")
    logInfo(s"Image Dimensions ${poolImage.metadata.xDim} ${poolImage.metadata.yDim} ${poolImage.metadata.numChannels}")

    assert(poolImage.metadata.xDim == image.metadata.xDim - convSize + 1, "Convolved image should have the right xDims.")
    assert(poolImage.metadata.yDim == image.metadata.yDim - convSize + 1, "Convolved image should have the right yDims.")
    assert(poolImage.metadata.numChannels == convBank.rows, "Convolved image should have the right num channels.")

  }
}
