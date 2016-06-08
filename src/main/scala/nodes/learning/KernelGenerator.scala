package nodes.learning

import scala.reflect.ClassTag

import breeze.linalg._
import breeze.math._
import breeze.numerics._

import org.apache.spark.rdd.RDD

import pipelines.Logging
import workflow.Transformer
import utils._

/**
 * Base trait for functions that compute a kernel on a given dataset.
 */
trait KernelGenerator[T] {

  /**
   * Create a kernel transformer using the given dataset as one of the arguments
   * to the kernel function. That is, if the kernel function is \phi(x, y), this
   * binds one of the arguments using the given data.
   *
   * @param trainData training data matrix to be used for kernel transformation
   * @return a kernel transformer that can be applied to train or test data
   */
  def fit(trainData: RDD[T]): KernelTransformer[T]
}


/**
 * Gaussian (RBF) kernel generator. The RBF kernel on two samples x, y is 
 * K(x, y) = exp(-||x - y||^2 * gamma)
 */
class GaussianKernelGenerator(gamma: Double, cacheKernel: Boolean = false)
    extends KernelGenerator[DenseVector[Double]] with Serializable with Logging {

  def fit(trainData: RDD[DenseVector[Double]]): GaussianKernelTransformer = {
    val trainDotProd = trainData.map(x => (x.t * x).toDouble)
    new GaussianKernelTransformer(trainData, trainDotProd, gamma, cacheKernel)
  }
}

/**
 * Base trait for functions that transform the given input data with respect
 * to a pre-specified kernel function.
 */
trait KernelTransformer[T] {

  /**
   * Perform kernel transformation of a given dataset.
   * If the kernel function is \phi, this computes \phi(x, y) for all 
   * rows in the given dataset.
   * 
   * @param data input data to compute kernel on.
   * @return a kernel matrix containing the output of the transformation
   */
  def apply(data: RDD[T]): KernelMatrix

  /**
   * Perform a kernel transformation on a single element.
   *
   * @param data input data to compute kernel on.
   * @return a dense vector containing the transformed data.
   */
  def apply(data: T): DenseVector[Double]

  // Internal function used to lazily populate the kernel matrix.
  private[learning] def computeKernel(data: RDD[T], idxs: Seq[Int]): RDD[DenseVector[Double]]
}

class GaussianKernelTransformer(
    trainData: RDD[DenseVector[Double]],
    trainDotProd: RDD[Double],
    gamma: Double,
    cacheKernel: Boolean) extends KernelTransformer[DenseVector[Double]] with Serializable {

  def apply(data: RDD[DenseVector[Double]]): KernelMatrix = {
    new BlockKernelMatrix(this, data, cacheKernel)
  }

  def apply(data: DenseVector[Double]): DenseVector[Double] = {
    // Compute data * trainData.t by doing an outer product and then summation
    // trainData is nxd and data is dx1 so the output here is a nx1 vector
    val dataBC = trainData.context.broadcast(data)
    val xyt = DenseVector(trainData.map { trainRow => 
      (trainRow.t * dataBC.value).toDouble
    }.collect())

    // Compute x * x.t (which is a scalar here)
    val dataNorm = (data.t * data).toDouble

    // Compute y * y.t (which is a vector of size nx1)
    val trainDataNorms = DenseVector(trainData.map { trainRow =>
      (trainRow.t * trainRow).toDouble
    }.collect())

    xyt *= (-2.0)
    xyt += dataNorm 
    xyt += trainDataNorms 

    xyt *= -gamma
    exp.inPlace(xyt)
    xyt
  }

  def computeKernel(
      data: RDD[DenseVector[Double]],
      blockIdxs: Seq[Int])
    : RDD[DenseVector[Double]] = {

    // Dot product of rows of X with each other
    val dataDotProd = if (data.id == trainData.id) { 
      trainDotProd
    } else {
      data.map(x => (x.t * x).toDouble)
    }

    // Extract a b x d block of training data
    val blockIdxSet = blockIdxs.toSet

    val blockDataArray = trainData.zipWithIndex.filter { case (vec, idx) =>
      blockIdxSet.contains(idx.toInt)
    }.map(x => x._1).collect()

    val blockData = MatrixUtils.rowsToMatrix(blockDataArray)
    assert(blockData.rows == blockIdxs.length)
    val blockDataBC = data.context.broadcast(blockData)

    // <xi,xj> for i in [nTest], j in blockIdxs
    val blockXXT = data.mapPartitions { itr  =>
      val bd = blockDataBC.value
      val vecMat = MatrixUtils.rowsToMatrix(itr)
      Iterator.single(vecMat*bd.t)
    }

    val trainBlockDotProd = DenseVector(trainDotProd.zipWithIndex.filter { case (vec, idx) =>
      blockIdxSet.contains(idx.toInt)
    }.map(x => x._1).collect())
    val trainBlockDotProdBC = data.context.broadcast(trainBlockDotProd)

    val kBlock = blockXXT.zipPartitions(dataDotProd) { case (iterXXT, iterTestDotProds) =>
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

    // TODO: We can't unpersist these broadcast variables until we have cached the
    // kernel block matrices ?
    // We could introduce a new clean up method here ?

    // blockDataBC.unpersist()
    // trainBlockDotProdBC.unpersist()
    kBlock
  }
}
