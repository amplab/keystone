package nodes.misc

import breeze.linalg._
import breeze.numerics.cos
import breeze.stats._
import breeze.stats.distributions.Rand
import org.scalatest.FunSuite
import utils.Stats


class CosineRandomFeaturesSuite extends FunSuite {
  val gamma = 1.34
  val numInputFeatures = 400
  val numOutputFeatures = 1000

  test("Guassian cosine random features") {
    val rf = CosineRandomFeatures.createGaussianCosineRF(numInputFeatures, numOutputFeatures, gamma)

    // Check that b is uniform
    assert(max(rf.b) <= 2*math.Pi)
    assert(min(rf.b) >= 0)
    assert(rf.b.size == numOutputFeatures)

    // Check that W is gaussian
    assert(rf.W.rows == numOutputFeatures)
    assert(rf.W.cols == numInputFeatures)
    assert(Stats.aboutEq(mean(rf.W),0, 10e-3 * gamma))
    assert(Stats.aboutEq(variance(rf.W), gamma * gamma, 10e-3 * gamma * gamma))

    //check the mapping
    val in = Stats.randMatrixUniform(1, numInputFeatures).toDenseVector
    val out = cos((in.t * rf.W.t).t + rf.b)
    assert(Stats.aboutEq(rf(in), out, 10e-3))
  }

  test("Cauchy cosine random features") {
    val rf = CosineRandomFeatures.createGaussianCosineRF(numInputFeatures, numOutputFeatures, gamma)

    // Check that b is uniform
    assert(max(rf.b) <= 2*math.Pi)
    assert(min(rf.b) >= 0)
    assert(rf.b.size == numOutputFeatures)

    // Check that W is cauchy
    assert(rf.W.rows == numOutputFeatures)
    assert(rf.W.cols == numInputFeatures)
    assert(Stats.aboutEq(median(rf.W),0,10e-3 * gamma))
    assert(Stats.aboutEq(variance(rf.W), gamma * gamma, 10e-3 * gamma * gamma))

    //check the mapping
    val in = Stats.randMatrixUniform(1, numInputFeatures).toDenseVector
    val out = cos((in.t * rf.W.t).t + rf.b)
    assert(Stats.aboutEq(rf(in), out, 10e-3))
  }
}
