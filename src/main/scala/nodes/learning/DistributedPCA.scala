package nodes.learning

import breeze.linalg._
import breeze.numerics._
import breeze.stats._
import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import org.apache.spark.rdd.RDD
import org.netlib.util.intW
import pipelines._
import utils.MatrixUtils
import workflow.{Transformer, Estimator}

import edu.berkeley.cs.amplab.mlmatrix.{RowPartition, NormalEquations, RowPartitionedMatrix, TSQR}

/**
 * Estimates a PCA model for dimensionality reduction using a distributedQR.
 *
 * @param dims Dimensions to reduce input dataset to.
 */
class DistributedPCAEstimator(dims: Int) extends Estimator[DenseVector[Float], DenseVector[Float]] with Logging {

  /**
   * Adapted from the "PCA2" matlab code given in appendix B of this paper:
   *    https://www.cs.princeton.edu/picasso/mats/PCA-Tutorial-Intuition_jp.pdf
   *
   * @param samples Features to be reduced. Logically row-major.
   * @return A PCA model which will perform dimensionality reduction when applied to data.
   */
  def fit(samples: RDD[DenseVector[Float]]): PCATransformer = {
    new PCATransformer(computePCA(samples, dims))
  }

  def computePCA(dataMat: RDD[DenseVector[Float]], dims: Int): DenseMatrix[Float] = {

    val mat = new RowPartitionedMatrix(dataMat.mapPartitions { part =>
      val dblIter = part.map(x => convert(x, Double))
      MatrixUtils.rowsToMatrixIter(dblIter).map(RowPartition(_))
    })
    val means = DenseVector(mat.colSums():_*) :/ mat.numRows().toDouble

    val meansBC = dataMat.context.broadcast(means)
    val zeroMeanMat = new RowPartitionedMatrix(mat.rdd.map { part =>
      RowPartition(part.mat(*, ::) - meansBC.value)
    })

    val rPart = new TSQR().qrR(zeroMeanMat)

    val svd.SVD(u, s, pcaT) = svd(rPart)

    val pca = convert(pcaT.t, Float)

    val matlabConventionPCA = PCAEstimator.enforceMatlabPCASignConvention(pca)

    // Return a subset of the columns.
    matlabConventionPCA(::, 0 until dims)
  }
}
