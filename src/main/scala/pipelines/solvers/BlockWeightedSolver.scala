package pipelines.solvers

import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import pipelines.Logging

import loaders.CsvFileDataLoader
import nodes.learning._
import nodes.util.TopKClassifier
import utils.Stats

object BlockWeightedSolver extends Serializable with Logging {
  val appName = "BlockWeightedSolver"

  def run(sc: SparkContext, conf: BlockWeightedSolverConfig) {
    // Load the data
    val trainingFeatures = CsvFileDataLoader(
      sc,
      conf.trainFeaturesDir).cache().setName("trainFeatures")
    val trainingLabels = CsvFileDataLoader(
      sc,
      conf.trainLabelsDir).cache().setName("trainLabels")

    val testFeatures = CsvFileDataLoader(
      sc,
      conf.testFeaturesDir).cache().setName("testFeatures")

    val testActual = CsvFileDataLoader(
      sc,
      conf.testActualDir).map(x => Array(x(0).toInt)).cache().setName("testActual")

    val numTrainImgs = trainingFeatures.count
    val numTestImgs = testFeatures.count

    // Fit a weighted least squares model to the data.
    val model = new BlockWeightedLeastSquaresEstimator(
      4096, 1, conf.lambda, conf.mixtureWeight).fit(
        trainingFeatures, trainingLabels)

    // Apply the model to test data and compute test error
    val testPredictedValues = model(testFeatures)
    val testPredicted = TopKClassifier(5).apply(testPredictedValues)
    logInfo("Top-5 TEST Error is " + Stats.getErrPercent(testPredicted, testActual, numTestImgs) + "%")

    val testPredictedTop1 = TopKClassifier(1).apply(testPredictedValues)
    logInfo("Top-1 TEST Error is " + Stats.getErrPercent(testPredictedTop1, testActual, numTestImgs) + "%")
  }

  case class BlockWeightedSolverConfig(
    trainFeaturesDir: String = "",
    trainLabelsDir: String = "",
    testFeaturesDir: String = "",
    testActualDir: String = "",
    lambda: Double = 6e-5,
    mixtureWeight: Double = 0.25)

  def parse(args: Array[String]): BlockWeightedSolverConfig = {
    new OptionParser[BlockWeightedSolverConfig](appName) {
      head(appName, "0.1")
      help("help") text("prints this usage text")
      opt[String]("trainFeaturesDir") required() action { (x,c) => c.copy(trainFeaturesDir=x) }
      opt[String]("trainLabelsDir") required() action { (x,c) => c.copy(trainLabelsDir=x) }
      opt[String]("testFeaturesDir") required() action { (x,c) => c.copy(testFeaturesDir=x) }
      opt[String]("testActualDir") required() action { (x,c) => c.copy(testActualDir=x) }

      // Solver params
      opt[Double]("lambda") action { (x,c) => c.copy(lambda=x) }
      opt[Double]("mixtureWeight") action { (x,c) => c.copy(mixtureWeight=x) }
    }.parse(args, BlockWeightedSolverConfig()).get
  }

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   * @param args
   */
  def main(args: Array[String]) = {
    val appConfig = parse(args)

    val conf = new SparkConf().setAppName(appName)
    conf.setIfMissing("spark.master", "local[2]")
    val sc = new SparkContext(conf)
    run(sc, appConfig)

    sc.stop()
  }
}
