package nodes.learning

import breeze.linalg.{DenseVector, Vector, argmax}
import breeze.stats.distributions.Multinomial
import org.apache.spark.SparkContext
import org.scalatest.FunSuite
import pipelines.LocalSparkContext

import scala.util.Random

object NaiveBayesModelSuite {

  private def calcLabel(p: Double, pi: Array[Double]): Int = {
    var sum = 0.0
    for (j <- 0 until pi.length) {
      sum += pi(j)
      if (p < sum) return j
    }
    -1
  }

  // Generate input of the form Y = (theta * x).argmax()
  def generateNaiveBayesInput(
      pi: Array[Double],            // 1XC
      theta: Array[Array[Double]],  // CXD
      nPoints: Int,
      seed: Int,
      modelType: String = "Multinomial",
      sample: Int = 10)
    : Seq[(Int, DenseVector[Double])] = {
    val D = theta(0).length
    val rnd = new Random(seed)
    val _pi = pi.map(math.pow(math.E, _))
    val _theta = theta.map(row => row.map(math.pow(math.E, _)))

    for (i <- 0 until nPoints) yield {
      val y = calcLabel(rnd.nextDouble(), _pi)
      val xi = modelType match {
        case "Bernoulli" => Array.tabulate[Double] (D) { j =>
          if (rnd.nextDouble () < _theta(y)(j) ) 1 else 0
        }
        case "Multinomial" =>
          val mult = Multinomial(DenseVector(_theta(y)))
          val emptyMap = (0 until D).map(x => (x, 0.0)).toMap
          val counts = emptyMap ++ mult.sample(sample).groupBy(x => x).map {
            case (index, reps) => (index, reps.size.toDouble)
          }
          counts.toArray.sortBy(_._1).map(_._2)
        case _ =>
          // This should never happen.
          throw new UnknownError(s"NaiveBayesSuite found unknown ModelType: $modelType")
      }

      (y, DenseVector(xi))
    }
  }

  /** Multinomial NaiveBayes with binary labels, 3 features */
  private val binaryMultinomialModel = new NaiveBayesModel(labels = Array(0, 1),
    pi = Array(0.2, 0.8), theta = Array(Array(0.1, 0.3, 0.6), Array(0.2, 0.4, 0.4)))
}

class NaiveBayesModelSuite extends FunSuite with LocalSparkContext {

  def validatePrediction(predictions: Seq[Int], input: Seq[(Int, DenseVector[Double])]) {
    val numOfPredictions = predictions.zip(input).count {
      case (prediction, expected) =>
        prediction != expected._1
    }
    assert(numOfPredictions < input.length / 5, "At least 80% of the predictions should be on.")
  }

  def validateModelFit(
      piData: Array[Double],
      thetaData: Array[Array[Double]],
      model: NaiveBayesModel): Unit = {
    def closeFit(d1: Double, d2: Double, precision: Double): Boolean = {
      (d1 - d2).abs <= precision
    }
    val modelIndex = (0 until piData.length).zip(model.labels.map(_.toInt))
    for (i <- modelIndex) {
      assert(closeFit(math.exp(piData(i._2)), math.exp(model.pi(i._1)), 0.05))
    }
    for (i <- modelIndex) {
      for (j <- 0 until thetaData(i._2).length) {
        assert(closeFit(math.exp(thetaData(i._2)(j)), math.exp(model.theta(i._1)(j)), 0.05))
      }
    }
  }

  test("Naive Bayes Multinomial") {
    sc = new SparkContext("local", "test")

    val nPoints = 1000
    val pi = Array(0.5, 0.1, 0.4).map(math.log)
    val theta = Array(
      Array(0.70, 0.10, 0.10, 0.10), // label 0
      Array(0.10, 0.70, 0.10, 0.10), // label 1
      Array(0.10, 0.10, 0.70, 0.10)  // label 2
    ).map(_.map(math.log))

    val testData = NaiveBayesModelSuite.generateNaiveBayesInput(
      pi, theta, nPoints, 42, "Multinomial")
    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    val model = NaiveBayesEstimator(3, 1.0).fit(testRDD.map(_._2), testRDD.map(_._1))
    validateModelFit(pi, theta, model)

    val validationData = NaiveBayesModelSuite.generateNaiveBayesInput(
      pi, theta, nPoints, 17, "Multinomial")
    val validationRDD = sc.parallelize[(Int, Vector[Double])](validationData, 2)

    // Test prediction on RDD.
    validatePrediction(model.apply(validationRDD.map(_._2)).map(x => argmax(x)).collect(), validationData)
  }
}
