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

  /**
   * Internal function used to lazily populate the kernel matrix.
   *
   * NOTE: This function returns a *cached* RDD and the caller is responsible
   * for managing its life cycle.
   *
   * @param data RDD to compute kernel on
   * @param idxs column indexes to use for computation
   * @return a pair containing the RDD for the column block and the diagonal block
   */
  private[learning] def computeKernel(
    data: RDD[T],
    idxs: Seq[Int]): (RDD[DenseMatrix[Double]], DenseMatrix[Double])
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

    dataBC.unpersist(true)

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
    : (RDD[DenseMatrix[Double]], DenseMatrix[Double]) = {

    // Dot product of rows of X with each other
    val dataDotProd = if (data.id == trainData.id) {
      trainDotProd
    } else {
      data.map(x => (x.t * x).toDouble)
    }

    // Extract a b x d block of training data
    val blockIdxSet = blockIdxs.toSet

    val trainBlockArray = trainData.zipWithIndex.filter { case (vec, idx) =>
      blockIdxSet.contains(idx.toInt)
    }.map(x => x._1).collect()

    val trainBlock = MatrixUtils.rowsToMatrix(trainBlockArray)
    assert(trainBlock.rows == blockIdxs.length)
    val trainBlockBC = data.context.broadcast(trainBlock)

    // <xi,xj> for i in [nTest], j in blockIdxs
    val blockXXT = data.mapPartitions { itr  =>
      val bd = trainBlockBC.value
      if (itr.hasNext) {
        val vecMat = MatrixUtils.rowsToMatrix(itr)
        Iterator.single(vecMat * bd.t)
      } else {
        Iterator.empty
      }
    }

    val trainBlockDotProd = DenseVector(trainDotProd.zipWithIndex.filter { case (vec, idx) =>
      blockIdxSet.contains(idx.toInt)
    }.map(x => x._1).collect())
    val trainBlockDotProdBC = data.context.broadcast(trainBlockDotProd)

    val kBlock = blockXXT.zipPartitions(dataDotProd) { case (iterXXT, iterDataDotProds) =>
      if (iterXXT.hasNext) {
        val xxt = iterXXT.next()
        assert(iterXXT.isEmpty)
        iterDataDotProds.zipWithIndex.map { case (dataDotProdVal, idx) =>
          // To compute |X_i - X_j|^2 we break it down into three terms
          // X_i^2 - 2*X_i*X_j + X_j^2
          val term1 = DenseVector.fill(xxt.cols)(dataDotProdVal)
          val term2 = xxt(idx, ::).t * (-2.0)
          val term3 = trainBlockDotProdBC.value
          val sum = (term1 + term2 + term3) * (-gamma)
          exp(sum)
        }
      } else {
        Iterator.empty
      }
    }

    val kBlockMat = MatrixUtils.rowsToMatrix(kBlock)

    kBlockMat.cache()
    kBlockMat.count

    trainBlockBC.unpersist(true)
    trainBlockDotProdBC.unpersist(true)

    val diagBlock = if (data.id == trainData.id) {
      // For train data use locally available data to compute diagonal block
      val kBlockBlock = trainBlock * trainBlock.t
      kBlockBlock :*= (-2.0)
      kBlockBlock(::, *) :+= trainBlockDotProd
      kBlockBlock(*, ::) :+= trainBlockDotProd
      kBlockBlock *= -gamma
      exp.inPlace(kBlockBlock)
      kBlockBlock
    } else {
      // For test data extract the diagonal block from the cached block
      MatrixUtils.rowsToMatrix(kBlockMat.flatMap { x =>
        MatrixUtils.matrixToRowArray(x)
      }.zipWithIndex.filter { case (vec, idx) =>
        blockIdxSet.contains(idx.toInt)
      }.map(x => x._1).collect().iterator)
    }

    (kBlockMat, diagBlock)
  }
}
