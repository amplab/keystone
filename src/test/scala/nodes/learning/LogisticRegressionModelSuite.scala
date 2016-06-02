package nodes.learning

import breeze.linalg.DenseVector
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines.PipelineContext
import utils.{MLlibUtils, Stats}

import scala.util.Random
import scala.util.control.Breaks._


object LogisticRegressionModelSuite {
  // Generate input of the form Y = logistic(offset + scale*X)
  def generateLogisticInput(
      offset: Double,
      scale: Double,
      nPoints: Int,
      seed: Int): Seq[(Int, DenseVector[Double])] = {
    val rnd = new Random(seed)
    val x1 = Array.fill[Double](nPoints)(rnd.nextGaussian())

    val y = (0 until nPoints).map { i =>
      val p = 1.0 / (1.0 + math.exp(-(offset + scale * x1(i))))
      if (rnd.nextDouble() < p) 1 else 0
    }

    val testData = (0 until nPoints).map(i => (y(i), DenseVector(Array(x1(i)))))
    testData
  }

  /**
   * Generates `k` classes multinomial synthetic logistic input in `n` dimensional space given the
   * model weights and mean/variance of the features. The synthetic data will be drawn from
   * the probability distribution constructed by weights using the following formula.
   *
   * P(y = 0 | x) = 1 / norm
   * P(y = 1 | x) = exp(x * w_1) / norm
   * P(y = 2 | x) = exp(x * w_2) / norm
   * ...
   * P(y = k-1 | x) = exp(x * w_{k-1}) / norm
   * where norm = 1 + exp(x * w_1) + exp(x * w_2) + ... + exp(x * w_{k-1})
   *
   * @param weights matrix is flatten into a vector; as a result, the dimension of weights vector
   *                will be (k - 1) * (n + 1) if `addIntercept == true`, and
   *                if `addIntercept != true`, the dimension will be (k - 1) * n.
   * @param xMean the mean of the generated features. Lots of time, if the features are not properly
   *              standardized, the algorithm with poor implementation will have difficulty
   *              to converge.
   * @param xVariance the variance of the generated features.
   * @param addIntercept whether to add intercept.
   * @param nPoints the number of instance of generated data.
   * @param seed the seed for random generator. For consistent testing result, it will be fixed.
   */
  def generateMultinomialLogisticInput(
      weights: Array[Double],
      xMean: Array[Double],
      xVariance: Array[Double],
      addIntercept: Boolean,
      nPoints: Int,
      seed: Int): Seq[(Int, DenseVector[Double])] = {
    val rnd = new Random(seed)

    val xDim = xMean.length
    val xWithInterceptsDim = if (addIntercept) xDim + 1 else xDim
    val nClasses = weights.length / xWithInterceptsDim + 1

    val x = Array.fill[DenseVector[Double]](nPoints)(DenseVector(Array.fill[Double](xDim)(rnd.nextGaussian())))

    x.foreach { vector =>
      // This doesn't work if `vector` is a sparse vector.
      val vectorArray = vector.toArray
      var i = 0
      val len = vectorArray.length
      while (i < len) {
        vectorArray(i) = vectorArray(i) * math.sqrt(xVariance(i)) + xMean(i)
        i += 1
      }
    }

    val y = (0 until nPoints).map { idx =>
      val xArray = x(idx).toArray
      val margins = Array.ofDim[Double](nClasses)
      val probs = Array.ofDim[Double](nClasses)

      for (i <- 0 until nClasses - 1) {
        for (j <- 0 until xDim) margins(i + 1) += weights(i * xWithInterceptsDim + j) * xArray(j)
        if (addIntercept) margins(i + 1) += weights((i + 1) * xWithInterceptsDim - 1)
      }
      // Preventing the overflow when we compute the probability
      val maxMargin = margins.max
      if (maxMargin > 0) for (i <- 0 until nClasses) margins(i) -= maxMargin

      // Computing the probabilities for each class from the margins.
      val norm = {
        var temp = 0.0
        for (i <- 0 until nClasses) {
          probs(i) = math.exp(margins(i))
          temp += probs(i)
        }
        temp
      }
      for (i <- 0 until nClasses) probs(i) /= norm

      // Compute the cumulative probability so we can generate a random number and assign a label.
      for (i <- 1 until nClasses) probs(i) += probs(i - 1)
      val p = rnd.nextDouble()
      var y = 0
      breakable {
        for (i <- 0 until nClasses) {
          if (p < probs(i)) {
            y = i
            break
          }
        }
      }
      y
    }

    val testData = (0 until nPoints).map(i => (y(i), x(i)))
    testData
  }
}


class LogisticRegressionModelSuite extends FunSuite with PipelineContext {
  def validatePrediction(
      predictions: Seq[Double],
      input: Seq[Double],
      expectedAcc: Double) {
    val numOffPredictions = predictions.zip(input).count { case (prediction, expected) =>
      prediction != expected
    }
    assert(((input.length - numOffPredictions).toDouble / input.length) > expectedAcc)
  }

  // Test if we can correctly learn A, B where Y = logistic(A + B*X)
  test("logistic regression with LBFGS") {
    sc = new SparkContext("local", "test")

    val nPoints = 10000
    val A = 0.0
    val B = -0.8

    val testData = LogisticRegressionModelSuite.generateLogisticInput(A, B, nPoints, 42)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()
    val lr = LogisticRegressionEstimator[DenseVector[Double]](2)

    val model = lr.fit(testRDD.map(_._2), testRDD.map(_._1))

    // Test the weights
    assert(Stats.aboutEq(model.model.weights(0), B, 0.03))
    assert(Stats.aboutEq(model.model.intercept, A, 0.02))

    val validationData = LogisticRegressionModelSuite.generateLogisticInput(A, B, nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)
    // Test prediction on RDD. Expected accuracy w/o intercept is 65%, should be 83% w/ intercept.
    validatePrediction(model.apply(validationRDD.map(_._2)).collect(), validationData.map(_._1.toDouble), 0.65)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.apply(row._2)), validationData.map(_._1.toDouble), 0.65)

    // Only the initial RDD should be cached, the estimator shouldn't force cache.
    assert(sc.getRDDStorageInfo.length == 1)
  }

  test("multinomial logistic regression with LBFGS") {
    sc = new SparkContext("local", "test")

    val nPoints = 10000

    /**
     * The following weights and xMean/xVariance are computed from iris dataset with lambda = 0.2.
     * As a result, we are actually drawing samples from probability distribution of built model.
     */
    val weights = Array(
      -0.57997, 0.912083, -0.371077, -0.819866, 2.688191,
      -0.16624, -0.84355, -0.048509, -0.301789, 4.170682)

    val xMean = Array(5.843, 3.057, 3.758, 1.199)
    val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)

    val testData = LogisticRegressionModelSuite.generateMultinomialLogisticInput(
      weights, xMean, xVariance, addIntercept = false, nPoints, 42)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    val lr = LogisticRegressionEstimator[DenseVector[Double]](
      numClasses = 3,
      numIters = 200,
      convergenceTol = 1E-15)
    val model = lr.fit(testRDD.map(_._2), testRDD.map(_._1))

    val numFeatures = testRDD.map(_._2.size).first()

    val weightsR = DenseVector(Array(
      -0.5837166, 0.9285260, -0.3783612, -0.8123411, 2.6228269,
      -0.1691865, -0.811048, -0.0646380))

    assert(Stats.aboutEq(MLlibUtils.mllibVectorToDenseBreeze(model.model.weights), weightsR, 0.05))

    val validationData = LogisticRegressionModelSuite.generateMultinomialLogisticInput(
      weights, xMean, xVariance, addIntercept = false, nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)
    // The validation accuracy is not good since this model (even the original weights) doesn't have
    // very steep curve in logistic function so that when we draw samples from distribution, it's
    // very easy to assign to another labels. However, this prediction result is consistent to R.
    validatePrediction(model.apply(validationRDD.map(_._2)).collect(), validationData.map(_._1.toDouble), 0.47)

    // Only the initial RDD should be cached, the estimator shouldn't force cache.
    assert(sc.getRDDStorageInfo.length == 1)
  }
}