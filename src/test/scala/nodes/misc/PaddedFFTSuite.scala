package nodes.misc

import breeze.linalg._
import breeze.numerics.cos
import breeze.stats._
import breeze.stats.distributions.{CauchyDistribution, Rand}
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines.{Logging, LocalSparkContext}
import utils.Stats


class PaddedFFTSuite extends FunSuite with LocalSparkContext with Logging {
  test("Test PaddedFFT node") {
    sc = new SparkContext("local", "test")

    //Set up a test matrix.
    val ones = DenseVector.zeros[Double](100)
    val twos = DenseVector.zeros[Double](100)
    ones(0) = 1.0
    twos(2) = 1.0

    val x = sc.parallelize(Seq(twos,ones))
    val fftd = PaddedFFT(x).collect()

    logInfo("Twos first")
    val twosout = fftd(0)

    logInfo("Then ones")
    val onesout = fftd(1)

    // Proof by agreement w/ Matlab: Re(fft(c(0, 0, 1, rep(0, 125))))
    assert(twosout.length === 64)
    assert(Stats.aboutEq(twosout(0), 1.0))
    assert(Stats.aboutEq(twosout(16), 0.0))
    assert(Stats.aboutEq(twosout(32), -1.0))
    assert(Stats.aboutEq(twosout(48), 0.0))

    // Proof by agreement w/ Matlab: Re(fft(c(1, rep(0, 127))))
    assert(Stats.aboutEq(onesout, DenseVector.ones[Double](64)))
  }
}
