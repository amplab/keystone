package nodes.misc

import breeze.linalg._
import breeze.numerics.cos
import breeze.stats._
import breeze.stats.distributions.{CauchyDistribution, Rand}
import org.scalatest.FunSuite
import utils.Stats


class CosineRandomFeaturesSuite extends FunSuite {
  val gamma = 1.34
  val numInputFeatures = 400
  val numOutputFeatures = 1000

  test("Guassian cosine random features") {
    val rf = CosineRandomFeatures(numInputFeatures, numOutputFeatures, gamma)

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
    val in = DenseVector.rand(numInputFeatures, Rand.uniform)
    val out = cos((in.t * rf.W.t).t + rf.b)
    assert(Stats.aboutEq(rf(in), out, 10e-3))
  }

  test("Cauchy cosine random features") {
    val rf = CosineRandomFeatures(
      numInputFeatures,
      numOutputFeatures,
      gamma,
      new CauchyDistribution(0, 1))

    // Check that b is uniform
    assert(max(rf.b) <= 2*math.Pi)
    assert(min(rf.b) >= 0)
    assert(rf.b.size == numOutputFeatures)

    // Check that W is cauchy
    assert(rf.W.rows == numOutputFeatures)
    assert(rf.W.cols == numInputFeatures)
    assert(Stats.aboutEq(median(rf.W),0,10e-3 * gamma))

    //check the mapping
    val in = DenseVector.rand(numInputFeatures, Rand.uniform)
    val out = cos((in.t * rf.W.t).t + rf.b)
    assert(Stats.aboutEq(rf(in), out, 10e-3))
  }
}
