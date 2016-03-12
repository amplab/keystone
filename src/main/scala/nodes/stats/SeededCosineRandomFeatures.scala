package nodes.stats

import breeze.linalg.{*, DenseMatrix, DenseVector}
import breeze.stats.distributions._
import breeze.numerics.cos
import breeze.stats.distributions.Rand
import org.apache.spark.rdd.RDD
import utils.MatrixUtils
import org.apache.commons.math3.random.MersenneTwister
import workflow.Transformer

/**
 * Transformer to generate random cosine features from a feature vector.
 *
 * Like [[CosineRandomFeatures]] except generates the transformation matrices from a seed on the executors instead of
 * serializing and broadcasting the transformation.
 * Kernel trick to allow Linear Solver to learn cosine interaction terms of the input
 *
 * Generates a matrix W of dimension (# output features) by (# input features)
 * And a dense vector b of dimension (# output features)
 *
 * Maps input vector x to cos(x * transpose(W :* gamma) + (b :* (2*pi))).
 *
 * Warning: Many Breeze [[Rand]] constructors take a [[RandBasis]] as an implicit parameter that defaults
 * to the Breeze default [[RandBasis]]. Users of this class must make sure to explicitly pass in the seeded
 * [[RandBasis]] in the distribution construction parameters.
 *
 * @param numInputFeatures  The number of features in the input vectors
 * @param numOutputFeatures The number of features to generate per vector
 * @param gamma A constant to use
 * @param seed The seed to use.
 * @param wDist method to generate a distribution for W from a seeded RandBasis. Defaults to a standard normal
 * @param bDist method to generate a distribution for b from a seeded RandBasis. Defaults to a uniform distribution
 */
case class SeededCosineRandomFeatures(
  numInputFeatures: Int,
  numOutputFeatures: Int,
  gamma: Double,
  seed: Long,
  wDist: RandBasis => Rand[Double] = _.gaussian,
  bDist: RandBasis => Rand[Double] = _.uniform
) extends Transformer[DenseVector[Double], DenseVector[Double]] {

  override def apply(in: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {

    in.mapPartitions { part =>
      val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
      val actualWDist = wDist(randBasis)
      val actualBDist = bDist(randBasis)
      val W = DenseMatrix.rand(numOutputFeatures, numInputFeatures, actualWDist) :* gamma
      val b = DenseVector.rand(numOutputFeatures, actualBDist) :* (2*math.Pi)
      val data = MatrixUtils.rowsToMatrix(part)
      val features: DenseMatrix[Double] = data * W.t
      features(*,::) :+= b
      cos.inPlace(features)
      MatrixUtils.matrixToRowArray(features).iterator
    }
  }

  override def apply(in: DenseVector[Double]): DenseVector[Double] = {
    val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
    val actualWDist = wDist(randBasis)
    val actualBDist = bDist(randBasis)
    val W = DenseMatrix.rand(numOutputFeatures, numInputFeatures, actualWDist) :* gamma
    val b = DenseVector.rand(numOutputFeatures, actualBDist) :* (2*math.Pi)
    val features = (in.t * W.t).t
    features :+= b
    cos.inPlace(features)
    features
  }
}
