package keystoneml.nodes.learning

import breeze.linalg._
import breeze.numerics._
import breeze.stats._
import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import org.apache.spark.rdd.RDD
import org.netlib.util.intW
import keystoneml.pipelines._
import keystoneml.workflow.{Transformer, Estimator}

class ZCAWhitener(val whitener: DenseMatrix[Double], val means: DenseVector[Double])
  extends Transformer[DenseMatrix[Double],DenseMatrix[Double]] {

  def apply(in: DenseMatrix[Double]): DenseMatrix[Double] = {
    (in(*, ::) - means) * whitener
  }
}

/**
  * Computes a ZCA Whitener, which is intended to rotate an input dataset to identity covariance.
  * The "Z" in ZCA Whitening means that the solution will be as close to the original dataset as possible while having
  * this identity covariance property.
  *
  * See here for more details:
  * http://ufldl.stanford.edu/wiki/index.php/Whitening
  *
  * @param eps Regularization Parameter
  */
class ZCAWhitenerEstimator(val eps: Double = 0.1)
  extends Estimator[DenseMatrix[Double],DenseMatrix[Double]] {

  def fit(in: RDD[DenseMatrix[Double]]): ZCAWhitener = {
    fitSingle(in.first)
  }

  def fitSingle(in: DenseMatrix[Double]): ZCAWhitener = {
    val means = (mean(in(::, *))).t

    val whitener: DenseMatrix[Double] = {
      val inc = convert(in(*, ::) - means, Float)
      val rows = inc.rows
      val cols = inc.cols

      val s1 = DenseVector.zeros[Float](math.min(rows, cols))
      val v1 = DenseMatrix.zeros[Float](inc.cols, inc.cols)

      // Get optimal workspace size
      // we do this by sending -1 as lwork to the lapack function
      val scratch, work = new Array[Float](1)
      val info = new intW(0)

      lapack.sgesvd("N", "A", rows, cols, scratch, rows, scratch, null, 1, scratch, cols, work, -1, info)

      val lwork1 = work(0).toInt
      val workspace = new Array[Float](lwork1)

      // Perform the SVD with sgesvd
      lapack.sgesvd("N", "A", rows, cols, inc.copy.data, rows, s1.data, null, 1, v1.data, cols, workspace, workspace.length, info)

      val s2  = pow(s1, 2.0f) / (rows - 1.0f)

      val sn1 = diag((s2 + eps.toFloat) :^ -0.5f)

      // NOTE: sgesvd returns singular values in the opposite order (when compared to eigenvalues)
      // Thus we need v.t * s * v here ?
      val svdMat = v1.t * sn1 * v1

      convert(svdMat, Double)
    }

    new ZCAWhitener(whitener, means)

  }
}


