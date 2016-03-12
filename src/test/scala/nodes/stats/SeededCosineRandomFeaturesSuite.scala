package nodes.stats

import breeze.linalg._
import breeze.numerics.cos
import breeze.stats._
import breeze.stats.distributions.{CauchyDistribution, Rand}
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines.LocalSparkContext
import utils.Stats


class SeededCosineRandomFeaturesSuite extends FunSuite with LocalSparkContext {
  val gamma = 1.34
  val numInputFeatures = 400
  val numOutputFeatures = 1000
  val seed = 0

  test("Guassian seeded cosine random features") {
    sc = new SparkContext("local", "test")

    val rf = SeededCosineRandomFeatures(numInputFeatures, numOutputFeatures, gamma, 0)

    val vec = DenseVector.rand[Double](numInputFeatures)

    val localOut = rf(vec)
    val localOutRepeat = rf(vec)
    val sparkOut = rf(sc.parallelize(Seq(vec))).collect().head

    // Check that the output size is correct
    assert(localOut.length === numOutputFeatures)

    // Check that they all have the same value and the seed worked correctly
    assert(localOut === sparkOut)
    assert(localOutRepeat === localOut)
  }
}
