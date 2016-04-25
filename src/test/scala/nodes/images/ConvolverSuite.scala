package nodes.images

import java.io.File

import breeze.linalg._
import breeze.stats._
import org.scalatest.FunSuite

import pipelines.{Logging, LocalSparkContext}
import utils._
import org.apache.spark.SparkContext
import utils.ChannelMajorArrayVectorizedImage
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

    val image = new ChannelMajorArrayVectorizedImage(
      imgArr, ImageMetadata(imgWidth, imgHeight, imgChannels))

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

    val image = new ChannelMajorArrayVectorizedImage(
      imgArr, ImageMetadata(imgWidth, imgHeight, imgChannels))

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

  test("convolutions should match scipy") {
    val im = TestUtils.loadTestImage("images/gantrycrane.png")

    val kimg = new ChannelMajorArrayVectorizedImage(Array.fill[Double](27)(0), ImageMetadata(3,3,3))
    val kimg2 = new ChannelMajorArrayVectorizedImage(Array.fill[Double](27)(0), ImageMetadata(3,3,3))
    var i = 0
    for (
      x <- 0 until 3;
      y <- 0 until 3;
      c <- 0 until 3
    ) {
      kimg.put(x,y,2-c,i.toDouble) //Channel order is reversed to match python.
      i+=1
    }

    kimg2.put(0,0,0,1.0)
    kimg2.put(0,0,0,2.0)
    kimg2.put(2,0,1,1.0)

    val conv = Convolver(Array(kimg, kimg2), im.metadata, None, false, flipFilters = true)
    val convimg = conv(im)

    val testImgRaw = csvread(new File(TestUtils.getTestResourceFileName("images/convolved.gantrycrane.csv")))
    val testImg = new ColumnMajorArrayVectorizedImage(testImgRaw(::,2).toArray,
      ImageMetadata(max(testImgRaw(::,0)).toInt+1, max(testImgRaw(::,1)).toInt+1, 1))

    val chans = ImageUtils.splitChannels(convimg)

    val pix = for ( x <- 0 until testImg.metadata.xDim;
          y <- 0 until testImg.metadata.yDim
    ) yield {
      (testImg.get(x,y,0), chans(0).get(x,y,0))
    }

    ImageUtils.writeImage("test.gantrycrane.png", chans(0), true)

    assert(testImg.metadata == chans(0).metadata, "Convolved images should have same metadata.")
    assert(testImg.equals(chans(0)), "Convolved images should match.")

  }
  test("5x5 patches convolutions no stride") {
    val imgWidth = 256
    val imgHeight = 256
    val imgChannels = 3
    val numFilters = 2
    val convSize = 5

    val imageBGR  = TestUtils.loadTestImage("images/test.jpeg")

    val imArray = imageBGR.toArray

    val imArrayRGB  = TestUtils.BGRtoRGB(new DenseMatrix(3, 256*256, imArray)).data

     val image = new ChannelMajorArrayVectorizedImage(imArrayRGB, imageBGR.metadata)

    val theanoFile = new File(TestUtils.getTestResourceFileName("images/theano_conv2d_nostride.csv"))

    val convolvedImgRaw:Array[Double] = csvread(theanoFile).data

    val convolvedImg = new ColumnMajorArrayVectorizedImage(convolvedImgRaw, ImageMetadata(imgWidth - convSize + 1, imgHeight - convSize + 1, numFilters))

    val convBank = convert(new DenseMatrix(2, 5*5*3, (0 until 2*5*5*3).toArray.map(x => 1)), Double)

    val convolver = new Convolver(convBank, imgWidth, imgHeight, imgChannels, normalizePatches=false)

    val poolImage = convolver(image)

    logInfo(s"Image Dimensions ${poolImage.metadata.xDim} ${poolImage.metadata.yDim} ${poolImage.metadata.numChannels}")

    assert(poolImage.metadata.xDim == image.metadata.xDim - convSize + 1, "Convolved image should have the right xDims.")
    assert(poolImage.metadata.yDim == image.metadata.yDim - convSize + 1, "Convolved image should have the right yDims.")
    assert(poolImage.metadata.numChannels == convBank.rows, "Convolved image should have the right num channels.")
    assert(poolImage.equals(convolvedImg), "Convolved image should match theano convolution")
  }

  test("5x5 patches convolutions with stride = 2") {
    val imgWidth = 256
    val imgHeight = 256
    val imgChannels = 3
    val numFilters = 2
    val convSize = 5

    val imageBGR  = TestUtils.loadTestImage("images/test.jpeg")

    val imArray = imageBGR.toArray

    val imArrayRGB  = TestUtils.BGRtoRGB(new DenseMatrix(3, 256*256, imArray)).data

    val image = new ChannelMajorArrayVectorizedImage(imArrayRGB, imageBGR.metadata)

    val theanoFile = new File(TestUtils.getTestResourceFileName("images/theano_conv2d_2stride.csv"))

    val convolvedImgRaw:Array[Double] = csvread(theanoFile).data

    val convolvedImg = new ColumnMajorArrayVectorizedImage(convolvedImgRaw, ImageMetadata((imgWidth - convSize + 1)/2, (imgHeight - convSize + 1)/2, numFilters))

    val convBank = convert(new DenseMatrix(2, 5*5*3, (0 until 2*5*5*3).toArray.map(x => 1)), Double)

    val convolver = new Convolver(convBank, imgWidth, imgHeight, imgChannels, patchStride=2, normalizePatches=false)

    val poolImage = convolver(image)

    logInfo(s"Image Dimensions ${poolImage.metadata.xDim} ${poolImage.metadata.yDim} ${poolImage.metadata.numChannels}")

    assert(poolImage.metadata.xDim == (image.metadata.xDim - convSize + 1)/2, "Convolved image should have the right xDims.")
    assert(poolImage.metadata.yDim == (image.metadata.yDim - convSize + 1)/2, "Convolved image should have the right yDims.")
    assert(poolImage.metadata.numChannels == convBank.rows, "Convolved image should have the right num channels.")
    assert(poolImage.equals(convolvedImg), "Convolved image should match theano convolution")
  }
}
