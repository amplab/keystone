package nodes.learning

import breeze.linalg._
import breeze.numerics._
import breeze.stats._
import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import org.apache.spark.rdd.RDD
import org.netlib.util.intW
import pipelines._
import utils.MatrixUtils
import workflow.{Pipeline, OptimizableEstimator, Transformer, Estimator}


/**
 * Performs dimensionality reduction on an input dataset.
 *
 * @param pcaMat The PCA matrix - usually obtained via PCAEstimator.
 */
class PCATransformer(val pcaMat: DenseMatrix[Float]) extends Transformer[DenseVector[Float], DenseVector[Float]] {

  /**
   * Apply dimensionality reduction to a point.
   *
   * @param in A point.
   * @return Dimensionality reduced output.
   */
  def apply(in: DenseVector[Float]): DenseVector[Float] = {
    pcaMat.t * in
  }
}

/**
 * Performs dimensionality reduction on an input dataset where each input item is an NxD array and the
 * projection matrix is a DxK array.
 *
 * @param pcaMat A DxK projection matrix.
 */
case class BatchPCATransformer(pcaMat: DenseMatrix[Float]) extends Transformer[DenseMatrix[Float], DenseMatrix[Float]] with Logging {
  def apply(in: DenseMatrix[Float]): DenseMatrix[Float] = {
    logDebug(s"Multiplying pcaMat:(${pcaMat.rows}x${pcaMat.cols}), in: (${in.rows}x${in.cols})")
    pcaMat.t * in
  }
}

/**
 * Estimates a PCA model for dimensionality reduction based on a sample of a larger input dataset.
 * Treats each column of the input matrices like a separate DenseVector input to [[PCAEstimator]].
 *
 * @param dims Dimensions to reduce input dataset to.
 */
case class LocalColumnPCAEstimator(dims: Int) extends Estimator[DenseMatrix[Float], DenseMatrix[Float]]
  with CostModel {

  val pcaEstimator = new PCAEstimator(dims)

  def fit(data: RDD[DenseMatrix[Float]]): Transformer[DenseMatrix[Float], DenseMatrix[Float]] = {
    val singleTransformer = pcaEstimator.fit(data.flatMap(x => MatrixUtils.matrixToColArray(x)))
    BatchPCATransformer(singleTransformer.pcaMat)
  }

  override def cost(
    n: Long,
    d: Int,
    k: Int,
    sparsity: Double,
    numMachines: Int,
    cpuWeight: Double,
    memWeight: Double,
    networkWeight: Double): Double = {
    pcaEstimator.cost(n, d, k, sparsity, numMachines, cpuWeight, memWeight, networkWeight)
  }
}

/**
 * Estimates a PCA model for dimensionality reduction based on a sample of a larger input dataset,
 * using a distributed PCA algorithm.
 * Treats each column of the input matrices like a separate DenseVector input to [[DistributedPCAEstimator]].
 *
 * @param dims Dimensions to reduce input dataset to.
 */
case class DistributedColumnPCAEstimator(dims: Int) extends Estimator[DenseMatrix[Float], DenseMatrix[Float]]
  with CostModel {

  val pcaEstimator = new DistributedPCAEstimator(dims)

  def fit(data: RDD[DenseMatrix[Float]]): Transformer[DenseMatrix[Float], DenseMatrix[Float]] = {
    val singleTransformer = pcaEstimator.fit(data.flatMap(x => MatrixUtils.matrixToColArray(x)))
    BatchPCATransformer(singleTransformer.pcaMat)
  }

  override def cost(
    n: Long,
    d: Int,
    k: Int,
    sparsity: Double,
    numMachines: Int,
    cpuWeight: Double,
    memWeight: Double,
    networkWeight: Double): Double = {
    pcaEstimator.cost(n, d, k, sparsity, numMachines, cpuWeight, memWeight, networkWeight)
  }
}

/**
 * Estimates a PCA model for dimensionality reduction based on a sample of a larger input dataset.
 * Treats each column of the input matrices like a separate DenseVector input to [[PCAEstimator]] or
 * [[DistributedPCAEstimator]].
 *
 * Automatically decides between distributed and local implementations when node-level optimization is enabled.
 * The default weights were determined empirically via results run on a 16 r3.4xlarge node cluster.
 *
 * @param dims Dimensions to reduce input dataset to.
 * @param numMachines
 * @param cpuWeight
 * @param memWeight
 * @param networkWeight
 */
case class ColumnPCAEstimator(
    dims: Int,
    numMachines: Option[Int] = None,
    cpuWeight: Double = 3.8e-4,
    memWeight: Double = 2.9e-1,
    networkWeight: Double = 1.32)
  extends OptimizableEstimator[DenseMatrix[Float], DenseMatrix[Float]] {
  val localEstimator = new LocalColumnPCAEstimator(dims)
  val distributedEstimator = new DistributedColumnPCAEstimator(dims)
  val default = distributedEstimator

  def optimize(sample: RDD[DenseMatrix[Float]], numPerPartition: Map[Int, Int])
  : Estimator[DenseMatrix[Float], DenseMatrix[Float]] = {
    val numColsPerMatrix: Double = sample.map(_.cols.toDouble).sum() / sample.count()
    val n = (numColsPerMatrix * numPerPartition.values.sum).toInt
    val d = sample.first().rows

    val realNumMachines = numMachines.getOrElse {
      if (sample.sparkContext.getExecutorStorageStatus.length == 1) {
        1
      } else {
        sample.sparkContext.getExecutorStorageStatus.length - 1
      }
    }

    val localCost = localEstimator.cost(
      n, d, dims, 1.0, realNumMachines, cpuWeight, memWeight, networkWeight
    )
    val distributedCost = distributedEstimator.cost(
      n, d, dims, 1.0, realNumMachines, cpuWeight, memWeight, networkWeight
    )

    if (localCost < distributedCost) {
      localEstimator
    } else {
      distributedEstimator
    }
  }
}

/**
 * Estimates a PCA model for dimensionality reduction based on a sample of a larger input dataset.
 *
 * @param dims Dimensions to reduce input dataset to.
 */
class PCAEstimator(dims: Int) extends Estimator[DenseVector[Float], DenseVector[Float]]
  with CostModel with Logging {

  /**
   * Adapted from the "PCA2" matlab code given in appendix B of this paper:
   *    https://www.cs.princeton.edu/picasso/mats/PCA-Tutorial-Intuition_jp.pdf
   *
   * @param samples A sample of features to be reduced. Often O(1e6). Logically row-major.
   * @return A PCA Matrix which will perform dimensionality reduction when applied to a data matrix.
   */
  def fit(samples: RDD[DenseVector[Float]]): PCATransformer = {
    val samps = samples.collect.map(_.toArray)
    val dataMat: DenseMatrix[Float] = DenseMatrix(samps:_*)
    new PCATransformer(computePCA(dataMat, dims))
  }

  def computePCA(dataMat: DenseMatrix[Float], dims: Int): DenseMatrix[Float] = {
    logDebug(s"Size of dataMat: (${dataMat.rows}, ${dataMat.cols})")

    val means = (mean(dataMat(::, *))).t

    val data = dataMat(*, ::) - means

    val rows = dataMat.rows
    val cols = dataMat.cols

    val s1 = DenseVector.zeros[Float](math.min(data.rows, data.cols))
    val v1 = DenseMatrix.zeros[Float](data.cols, data.cols)

    // Get optimal workspace size
    // we do this by sending -1 as lwork to the lapack function
    val scratch, work = new Array[Float](1)
    val info = new intW(0)

    lapack.sgesvd("N", "A", rows, cols, scratch, rows, scratch, null, 1, scratch, cols, work, -1, info)

    val lwork1 = work(0).toInt
    val workspace = new Array[Float](lwork1)

    // Perform the SVD with sgesvd
    lapack.sgesvd("N", "A", rows, cols, data.toArray, rows, s1.data, null, 1, v1.data, cols, workspace, workspace.length, info)

    val pca = PCAEstimator.enforceMatlabPCASignConvention(v1.t)

    // Return a subset of the columns.
    pca(::, 0 until dims)
  }

  override def cost(
    n: Long,
    d: Int,
    k: Int,
    sparsity: Double,
    numMachines: Int,
    cpuWeight: Double,
    memWeight: Double,
    networkWeight: Double): Double = {
    val flops = n.toDouble * d * d
    val bytesScanned = n.toDouble * d
    val network = n.toDouble * d
    math.max(cpuWeight * flops, memWeight * bytesScanned) + networkWeight * network
  }
}

object PCAEstimator {
  /**
   * Mimic matlab sign convention for PCA matrices.
   * This is an in-place operation and will modify your input matrix.
   *
   * Enforces a sign convention on the coefficients -- the largest element in
   * each column will have a positive sign.
   *
   * @param pca
   * @return
   */
  def enforceMatlabPCASignConvention(pca: DenseMatrix[Float]): DenseMatrix[Float] = {
    val colMaxs = max(pca(::, *)).t.toArray
    val absPCA = abs(pca)
    val absColMaxs = max(absPCA(::, *)).t.toArray
    val signs = colMaxs.zip(absColMaxs).map { x =>
      if (x._1 == x._2) 1.0f else -1.0f
    }

    pca(*, ::) :* new DenseVector(signs)
  }
}