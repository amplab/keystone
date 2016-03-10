package nodes.images

import breeze.linalg._
import breeze.stats._
import org.scalatest.FunSuite
import pipelines.Logging
import utils._
import utils.TestUtils._

import scala.util.Random

class ImageBenchMarkSuite extends FunSuite with Logging {

  case class TestParam(name: String, size: (Int, Int, Int), kernelSize: Int, numKernels: Int, poolSize: Int, poolStride: Int)
  val tests = Array(
    TestParam("Cifar100", (32,32,3), 6, 100, 13, 14),
    TestParam("Cifar1000", (32,32,3), 6, 1000, 13, 14),
    TestParam("Cifar10000", (32,32,3), 6, 10000, 13, 14),
    TestParam("ImageNet", (256,256,3), 6, 100, (256-5)/2, (256-5)/2),
    TestParam("SolarFlares", (256,256,12), 6, 100, (256-5)/12, (256-5)/12),
    TestParam("ConvolvedSolarFlares", (251,251,100), 6, 100, 251/12, 251/12)
    //Todo (sparks) Figure out a good way to "uncomment" this via a config parameter when we want bigger tests.
    //TestParam("SolarFlares2", (256,256,12), 5, 1024, (256-4)/12, (256-4)/12)

  )

  def getImages(t: TestParam) = Array[VectorizedImage](
    genRowMajorArrayVectorizedImage(t.size._1, t.size._2, t.size._3),
    genColumnMajorArrayVectorizedImage(t.size._1, t.size._2, t.size._3),
    genChannelMajorArrayVectorizedImage(t.size._1, t.size._2, t.size._3)
  )

  test("Reverse map") {
    var img = genColumnMajorArrayVectorizedImage(7, 13, 17)
    val item = for(
      z <- 0 until img.metadata.numChannels;
      x <- 0 until img.metadata.xDim;
      y <- 0 until img.metadata.yDim
    ) yield img.get(x,y,z)
    assert(item.toArray === img.iter.map(_.v).toArray, "Column Major Array iterator does not produce the right order.")

    val img2 = genRowMajorArrayVectorizedImage(7, 13, 17)
    val item2 = for(
      z <- 0 until img2.metadata.numChannels;
      y <- 0 until img2.metadata.yDim;
      x <- 0 until img2.metadata.xDim
    ) yield img2.get(x,y,z)

    assert(item2.toArray === img2.iter.map(_.v).toArray, "Row Major Array iterator does not produce the right order.")

    val img3 = genChannelMajorArrayVectorizedImage(7, 13, 17)
    val item3 = for(
      y <- 0 until img3.metadata.yDim;
      x <- 0 until img3.metadata.xDim;
      z <- 0 until img3.metadata.numChannels
    ) yield img3.get(x,y,z)

    assert(item3.toArray === img3.iter.map(_.v).toArray, "Channel Major Array iterator does not produce the right order.")
  }


  test("Iteration Benchmarks") {
    def iterTimes(x: VectorizedImage) = {
      var sum1 = 0.0
      val data = (0 until x.metadata.xDim*x.metadata.yDim*x.metadata.numChannels).map(x.getInVector).toArray

      val start1 = System.nanoTime()
      var i = 0

      while (i < data.length) {
        sum1+=data(i)
        i+=1
      }
      val t1 = System.nanoTime() - start1


      var sum2 = 0.0
      val start2 = System.nanoTime()
      val iter = x.iter
      while (iter.hasNext) {
        sum2 += iter.next.v
      }
      val t2 = System.nanoTime() - start2

      val start3 = System.nanoTime()
      val tot = data.sum
      val t3 = System.nanoTime() - start3

      (t1, t2, t3, sum1, sum2, tot)
    }

    for (
      iter <- 1 to 5;
      t <- tests;
      i <- getImages(t)
    ) {
      val (t1, t2, t3, a, b, c) = iterTimes(i)
      val slowdown = t2.toDouble/t1
      val istr = '"' + i.toString + '"'
      logDebug(s"${t.name},$istr,$t1,$t2,$t3,$slowdown,${t2.toDouble/t3},$a,$b,$c")
    }
    //Iteration just going through the data.

  }

  test("Convolution Benchmarks") {
    def convTime(x: Image, t: TestParam) = {
      val filters = DenseMatrix.rand[Double](t.numKernels, t.kernelSize*t.kernelSize*t.size._3)
      val conv = new Convolver(filters, x.metadata.xDim, x.metadata.yDim, x.metadata.numChannels, normalizePatches = false)

      val start = System.nanoTime
      val res = conv(x)
      val elapsed = System.nanoTime - start

      elapsed
    }

    val res = for(
      iter <- 1 to 5;
      t <- tests
    ) yield {
      val img = genChannelMajorArrayVectorizedImage(t.size._1, t.size._2, t.size._3) //Standard grayScale format.

      val flops = (t.size._1.toLong-t.kernelSize+1)*(t.size._2-t.kernelSize+1)*
        t.size._3*t.kernelSize*t.kernelSize*
        t.numKernels

      val t1 = convTime(img, t)

      logDebug(s"${t.name},$t1,$flops,${2.0*flops.toDouble/t1}")
      (t.name, t1, flops, (2.0*flops.toDouble/t1))
    }
    val groups = res.groupBy(_._1)


    logInfo(Seq("name","max(flops)","median(flops)","stddev(flops)").mkString(","))
    groups.foreach { case (name, values) =>
      val flops = DenseVector(values.map(_._4):_*)
      val maxf = max(flops)
      val medf = median(flops)
      val stddevf = stddev(flops)
      logInfo(f"$name,$maxf%2.3f,$medf%2.3f,$stddevf%2.3f")
    }
  }
}
