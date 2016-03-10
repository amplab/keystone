package nodes.learning

import breeze.linalg._
import breeze.numerics._
import breeze.stats._
import breeze.stats.distributions.{Gaussian, ThreadLocalRandomGenerator, RandBasis}
import com.github.fommil.netlib.LAPACK._
import edu.berkeley.cs.amplab.mlmatrix.util.QRUtils
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.rdd.RDD
import org.netlib.util.intW
import pipelines.Logging
import workflow.Estimator

/**
 * Approximately estimates a PCA model for dimensionality reduction based on an input dataset.
 *
 * @param dims Dimensions to reduce input dataset to.
 * @param q The number of iterations to use
 * @param p The amount of padding to add beyond dims for the calculations
 */
class ApproximatePCAEstimator(dims: Int, q: Int = 10, p: Int = 5)
    extends Estimator[DenseVector[Float], DenseVector[Float]] with Logging {

  /**
   * Computes the PCA using a sketch-based algorithm.
   *
   * @param samples A sample of features to be reduced. Often O(1e6). Logically row-major.
   * @return A PCA Matrix which will perform dimensionality reduction when applied to a data matrix.
   */
  def fit(samples: RDD[DenseVector[Float]]): PCATransformer = {
    val samps = samples.collect.map(_.toArray)
    val dataMat: DenseMatrix[Float] = DenseMatrix(samps:_*)
    new PCATransformer(approximatePCA(dataMat, dims, q, p))
  }

  def approximatePCA(data: DenseMatrix[Float], k: Int, q: Int = 10, p: Int = 5): DenseMatrix[Float] = {
    //This algorithm corresponds to Algorithms 4.4 and 5.1 of Halko, Martinsson, and Tropp, 2011.
    //According to sections 9.3 and  9.4 of the same, Ming Gu argues for exponentially fast convergence.

    val A = convert(data, Double)

    val Q = ApproximatePCAEstimator.approximateQ(A, k+p, q)

    val B = Q.t * A //cpu: l*n*d, mem: l*d
    val usvt = svd.reduced(B) //cpu: l*d^2, mem: l*d
    val pca = convert(usvt.Vt.t, Float)
    logDebug(s"shape of pca (${pca.rows},${pca.cols}")

    val matlabConventionPCA = PCAEstimator.enforceMatlabPCASignConvention(pca)

    // Return a subset of the columns.
    matlabConventionPCA(::, 0 until k)
  }
}


object ApproximatePCAEstimator {
  /**
   * This corresponds to algorithm 4.4 of HMT2011.
   *
   * @param A Data matrix to sketch.
   * @param l Number of components in the sketch.
   * @param q Number of iterations to use in the computation.
   * @return An n x (k+p) matrix that approximates A
   */
  def approximateQ(A: DenseMatrix[Double], l: Int, q: Int, seed: Int = 0): DenseMatrix[Double] = {
    val d = A.cols

    val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
    val omega = DenseMatrix.rand(d, l, Gaussian(0,1)(randBasis)) //cpu: d*l, mem: d*l
    val y0 = A*omega //cpu: n*d*l, mem: n*l

    var Q = QRUtils.qrQR(y0)._1 //cpu: n*l**2

    for (i <- 1 to q) {
      val YHat = Q.t * A //cpu: l*n*d, mem: l*d
      val Qh = QRUtils.qrQR(YHat.t)._1 //cpu: d*l^2, mem: d*l

      val Yj = A * Qh //cpu: n*d*l, mem: n*l
      Q = QRUtils.qrQR(Yj)._1 //cpu:  n*l^2, mem: n*l
    }

    Q
  }
}