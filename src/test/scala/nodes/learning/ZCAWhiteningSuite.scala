package nodes.learning

import breeze.linalg._
import breeze.numerics._
import breeze.stats.distributions._
import org.scalatest.FunSuite
import pipelines._

class ZCAWhiteningSuite extends FunSuite with LocalSparkContext with Logging {

  val nrows = 10000
  val ndim = 10

  val x = DenseMatrix.rand[Double](nrows, ndim, Gaussian(0.0, 1.0))

  def fitAndCompare(x: DenseMatrix[Double], eps: Double, thresh: Double): Boolean = {
    val whitener = new ZCAWhitenerEstimator(eps).fitSingle(x)

    val wx = whitener(x)

    //Checks max(max(abs(cov(whiten(x))) - eye(10)) < sqrt(eps)
    max(abs(cov(convert(wx, Double))) - DenseMatrix.eye[Double](ndim)) < thresh
  }

  test("whitening with small epsilon") {
    assert(fitAndCompare(x, 1e-12, 1e-4),
      "Whitening the base matrix should produce unit variance and zero covariance.")
  }

  test("whitening with large epsilon") {
    assert(fitAndCompare(x, 0.1, 0.1),
      "Whitening the base matrix should produce unit variance and zero covariance.")

    assert(!fitAndCompare(x, 0.1, 1e-4),
      "Whitening the base matrix with a large epsilon should be somewhat noisy.")
  }
}
