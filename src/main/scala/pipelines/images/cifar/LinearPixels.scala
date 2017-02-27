package pipelines.images.cifar

import breeze.linalg.DenseVector
import evaluation.MulticlassClassifierEvaluator
import loaders.CifarLoader
import nodes.images.{GrayScaler, ImageExtractor, ImageVectorizer, LabelExtractor}
import nodes.learning.LinearMapEstimator
import nodes.util.{Cacher, ClassLabelIndicatorsFromIntLabels, MaxClassifier}
import org.apache.spark.{SparkConf, SparkContext}
import pipelines.Logging
import scopt.OptionParser
import utils.Image
import workflow.Pipeline


object LinearPixels extends Logging {
  val appName = "LinearPixels"
  case class LinearPixelsConfig(trainLocation: String = "", testLocation: String = "")

  def run(sc: SparkContext, config: LinearPixelsConfig): Pipeline[Image, Int] = {
    val numClasses = 10

    // Load and cache the training data.
    val trainData = CifarLoader(sc, config.trainLocation).cache()

    val trainImages = ImageExtractor(trainData)

    val labelExtractor = LabelExtractor andThen
        ClassLabelIndicatorsFromIntLabels(numClasses) andThen
        new Cacher[DenseVector[Double]]
    val trainLabels = labelExtractor(trainData)

    // A featurizer maps input images into vectors. For this pipeline, we'll also convert the image to grayscale.
    // We then estimate our model by calling a linear solver on our data.
    val predictionPipeline = GrayScaler andThen
      ImageVectorizer andThen
      (new LinearMapEstimator, trainImages, trainLabels) andThen
      MaxClassifier

    // Calculate training error.
    val evaluator = new MulticlassClassifierEvaluator(numClasses)
    val trainEval = evaluator.evaluate(predictionPipeline(trainImages), LabelExtractor(trainData))

    // Do testing.
    val testData = CifarLoader(sc, config.testLocation)
    val testImages = ImageExtractor(testData)
    val testLabels = labelExtractor(testData)

    val testEval = evaluator.evaluate(predictionPipeline(testImages), LabelExtractor(testData))

    logInfo(s"Training accuracy: \n${trainEval.totalAccuracy}")
    logInfo(s"Test accuracy: \n${testEval.totalAccuracy}")

    predictionPipeline
  }

  def parse(args: Array[String]): LinearPixelsConfig = new OptionParser[LinearPixelsConfig](appName) {
    head(appName, "0.1")
    help("help") text("prints this usage text")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("testLocation") required() action { (x,c) => c.copy(testLocation=x) }
  }.parse(args, LinearPixelsConfig()).get

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   *
   * @param args
   */
  def main(args: Array[String]) = {
    val appConfig = parse(args)

    val conf = new SparkConf().setAppName(appName)
    conf.setIfMissing("spark.master", "local[2]") // This is a fallback if things aren't set via spark submit.
    val sc = new SparkContext(conf)
    run(sc, appConfig)

    sc.stop()
  }

}
