package nodes.learning

import breeze.linalg._
import breeze.numerics._
import breeze.stats._
import com.github.fommil.netlib.LAPACK._
import edu.berkeley.cs.amplab.mlmatrix.util.QRUtils
import org.apache.spark.rdd.RDD
import org.netlib.util.intW
import pipelines.Logging
import workflow.Estimator

/**
 * Created by tomerk11 on 12/16/15.
 */
class ApproximatePCAEstimator(dims: Int, q: Int = 10, p: Int = 5) extends Estimator[DenseVector[Float], DenseVector[Float]] with Logging {

  /**
   * Adapted from the "PCA2" matlab code given in appendix B of this paper:
   *    https://www.cs.princeton.edu/picasso/mats/PCA-Tutorial-Intuition_jp.pdf
   *
   * @param samples A sample of features to be reduced. Often O(1e6). Logically row-major.
   * @return A PCA Matrix which will perform dimensionality reduction when applied to a data matrix.
   */
  def fit(samples: RDD[DenseVector[Float]]): PCATransformer = {
    val samps = samples.collect.map(_.toArray)
    val dataMat: DenseMatrix[Double] = convert(DenseMatrix(samps:_*), Double)
    new PCATransformer(convert(approximatePCA(dataMat, dims, q, p), Float))
  }

  def approximatePCA(data: DenseMatrix[Double], k: Int, q: Int = 10, p: Int = 5): DenseMatrix[Double] = {
    //This algorithm corresponds to Algorithms 4.4 and 5.1 of Halko, Martinsson, and Tropp, 2011.
    //According to sections 9.3 and  9.4 of the same, Ming Gu argues for exponentially fast convergence.

    val A = data
    val d = A.cols

    val l = k + p
    val omega = new DenseMatrix(d, l, randn(d*l).toArray) //cpu: d*l, mem: d*l
    val y0 = A*omega //cpu: n*d*l, mem: n*l

    var Q = QRUtils.qrQR(y0)._1 //cpu: n*l**2

    for (i <- 1 to q) {
      val YHat = Q.t * A //cpu: l*n*d, mem: l*d
      val Qh = QRUtils.qrQR(YHat.t)._1 //cpu: d*l^2, mem: d*l

      val Yj = A * Qh //cpu: n*d*l, mem: n*l
      Q = QRUtils.qrQR(Yj)._1 //cpu:  n*l^2, mem: n*l
    }

    val B = Q.t * A //cpu: l*n*d, mem: l*d
    val usvt = svd.reduced(B) //cpu: l*d^2, mem: l*d
    val pca = usvt.Vt.t
    logInfo(s"shape of pca (${pca.rows},${pca.cols}")

    // Mimic matlab
    // Enforce a sign convention on the coefficients -- the largest element in
    // each column will have a positive sign.

    val colMaxs = max(pca(::, *)).toArray
    val absPCA = abs(pca)
    val absColMaxs = max(absPCA(::, *)).toArray
    val signs = colMaxs.zip(absColMaxs).map { x =>
      if (x._1 == x._2) 1.0 else -1.0
    }

    pca(*, ::) :*= new DenseVector(signs)

    // Return a subset of the columns.
    pca(::, 0 until k)
  }
}
