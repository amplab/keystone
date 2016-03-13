package nodes.learning

import workflow.Transformer

import breeze.linalg._
import breeze.math._
import breeze.numerics._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import pipelines.Logging
import scala.reflect.ClassTag
import utils._


// Abstract class shared by Kernel functions
abstract class KernelGenerator extends Transformer[DenseVector[Double], DenseVector[Double]] {

  val trainDotProd:RDD[Double]

  /* Cached KernelBlockBlock (used for training), and corresponding
   * blockIdx
   */
  var cachedBlockBlock:Option[(DenseMatrix[Double], Seq[Int])] = None

  def apply(testMat: RDD[DenseVector[Double]], blockIdxs: Seq[Int], same: Boolean = false, cache: Boolean = false): RDD[DenseVector[Double]]


}



class GaussianKernelGenerator(gamma: Double, trainMat: RDD[DenseVector[Double]])
  extends KernelGenerator
  with Serializable with Logging {

  val trainDotProd = trainMat.map {x =>
    (x.t*x).toDouble
  }

  cachedBlockBlock = None

  /**
   * Return a column block of the kernel test matrix
   *
   * The kernel test matrix is a |test| x |train| matrix K_testtrain.
   * This function returns the columns in blockIdxs of K_testtrain.
   *
   * WARNING: Observe the order that test comes first
   */

  def apply(testMat: RDD[DenseVector[Double]], blockIdxs: Seq[Int], same: Boolean = false, cache: Boolean = false): RDD[DenseVector[Double]] = {
    logInfo("testMat.count=" + testMat.count +
      ",trainMat.count=" + trainMat.count +
      ", blockSize=" + blockIdxs.length)

    assert(KernelUtils.isSorted(blockIdxs)) // limitation for now
    // Dot product of rows of X with each other
    val testDotProd =
    if (same) {
      trainDotProd
    } else {
      testMat.map {x =>
        (x.t*x).toDouble
      }
    }
    val blockIdxSet = blockIdxs.toSet
    // b x d block of training data
    val blockDataArray = trainMat.zipWithIndex.filter{ case (vec, idx) =>
      blockIdxSet.contains(idx.toInt)
    }.map(x=> x._1).collect()
    val blockData = KernelUtils.rowsToMatrix(blockDataArray)
    assert(blockData.rows == blockIdxs.length)
    val blockDataBC = trainMat.context.broadcast(blockData)

    // <xi,xj> for i in [nTest], j in blockIdxs
    val blockXXT = testMat.mapPartitions { itr  =>
      val bd = blockDataBC.value
      val vecMat = KernelUtils.rowsToMatrix(itr)
      Iterator.single(vecMat*bd.t)
    }
    val trainBlockDotProd = DenseVector(trainDotProd.zipWithIndex.filter{ case (vec, idx) =>
      blockIdxSet.contains(idx.toInt)
    }.map(x => x._1).collect())
    val trainBlockDotProdBC = trainMat.context.broadcast(trainBlockDotProd)

    val kTestBlock = blockXXT.zipPartitions(testDotProd) { case (iterXXT, iterTestDotProds) =>
      val xxt = iterXXT.next()
      assert(iterXXT.isEmpty)
      iterTestDotProds.zipWithIndex.map { case (testDotProd, idx) =>
        val term1 = xxt(idx, ::).t * (-2.0)
        val term2 = DenseVector.fill(xxt.cols){testDotProd}
        val term3 = trainBlockDotProdBC.value
        val term4 = (term1 + term2 + term3) * (-gamma)
        exp(term4)
      }
    }

    kTestBlock.cache()
    kTestBlock.count
    blockDataBC.unpersist()
    trainBlockDotProdBC.unpersist()
    if (cache) {
      val kBlockBlock = blockData * blockData.t
      kBlockBlock :*= (-2.0)
      kBlockBlock(::, *) :+= trainBlockDotProd
      kBlockBlock(*, ::) :+= trainBlockDotProd
      cachedBlockBlock = Some((exp(kBlockBlock * -gamma), blockIdxs))
    }
    kTestBlock
  }

  def apply(testMat: DenseVector[Double]): DenseVector[Double] = {
    apply(trainMat.context.parallelize(Array(testMat)), (0 until testMat.size), false).collect()(0)
  }

}
