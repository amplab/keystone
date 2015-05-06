package pipelines.images.cifar

import nodes.CifarLoader
import nodes.images.{ImageVectorizer, LabelExtractor, ImageExtractor, GrayScaler}
import nodes.learning.{LinearMapEstimator, LinearMapper}
import nodes.util.{ClassLabelIndicatorsFromIntLabels, Cacher}
import org.apache.spark.{SparkContext, SparkConf}
import pipelines.Logging
import utils.Stats
import scopt.OptionParser

object LinearPixels extends Logging {
  val appName = "LinearPixels"

  def run(sc: SparkContext, trainFile: String, testFile: String) = {
    val numClasses = 10

    // Load and cache the training data.
    val trainData = CifarLoader(sc, trainFile).cache

    // A featurizer maps input images into vectors. For this pipeline, we'll also convert the image to grayscale.
    val featurizer = ImageExtractor then GrayScaler then ImageVectorizer
    val labelExtractor = LabelExtractor then ClassLabelIndicatorsFromIntLabels(numClasses) then new Cacher

    // Our training features are the featurizer applied to our training data.
    val trainFeatures = featurizer(trainData)
    val trainLabels = labelExtractor(trainData)

    // We estimate our model as by calling a linear solver on our data.
    val model = LinearMapEstimator().fit(trainFeatures, trainLabels)

    // The final prediction pipeline is the composition of our featurizer and our model.
    // Since we end up using the results of the prediction twice, we'll add a caching node.
    val predictionPipeline = featurizer then model then new Cacher

    // Calculate training error.
    val trainError = Stats.classificationError(predictionPipeline(trainData), trainLabels)

    // Do testing.
    val testData = CifarLoader(sc, testFile)
    val testLabels = labelExtractor(testData)

    val testError = Stats.classificationError(predictionPipeline(testData), testLabels)

    logInfo(s"Training error is: $trainError, Test error is: $testError")
  }

  // This is needed to support command line parsing.
  case class Config(trainFile: String = "", testFile: String = "")

  val parser = new OptionParser[Config](appName) {
    head(appName, "0.1")
    opt[String]('T', "trainFile") required() action { (x,c) => c.copy(trainFile=x) }
    opt[String]('t', "testFile") required() action { (x,c) => c.copy(testFile=x) }
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName(appName)
    val opts = parser.parse(args, Config())

    val sc = new SparkContext(conf)
    run(sc, opts.get.trainFile, opts.get.testFile)

    sc.stop()
  }
}