package nodes.learning

import breeze.linalg._
import breeze.numerics._
import breeze.stats.distributions._
import org.scalatest.FunSuite
import pipelines._

class ZCAWhiteningSuite extends FunSuite with LocalSparkContext with Logging {

  test("whitening") {
    val eps = 0.1
    val nrows = 10000
    val ndim = 10

    val x = DenseMatrix.rand[Double](nrows, ndim, Gaussian(0.0, 1.0))
    val whitener = new ZCAWhitenerEstimator(eps).fitSingle(x)

    val wx = whitener(x)

    //Checks max(max(abs(cov(whiten(x))) - eye(10)) < 2*eps
    assert(max(abs(cov(convert(wx, Double))) - DenseMatrix.eye[Double](ndim)) < 2*eps,
      "Whitening the base matrix should produce unit variance and zero covariance.")
  }
}
