package pipelines.text

import breeze.linalg.{Vector, argmax}
import evaluation.MulticlassClassifierEvaluator
import loaders.NewsgroupsDataLoader
import nodes.learning.NaiveBayesEstimator
import nodes.nlp._
import nodes.stats.TermFrequency
import nodes.util.{CommonSparseFeatures, MaxClassifier}
import org.apache.spark.{SparkConf, SparkContext}
import pipelines.Logging
import scopt.OptionParser

object NewsgroupsPipeline extends Logging {
  val appName = "NewsgroupsPipeline"

  def run(sc: SparkContext, conf: NewsgroupsConfig) {

    val trainData = NewsgroupsDataLoader(sc, conf.trainLocation)
    val numClasses = NewsgroupsDataLoader.classes.length

    // Build the classifier estimator
    logInfo("Training classifier")
    val predictor = Trim.then(LowerCase())
        .then(Tokenizer())
        .then(new NGramsFeaturizer(1 to conf.nGrams))
        .then(TermFrequency(x => 1))
        .thenEstimator(CommonSparseFeatures(conf.commonFeatures))
        .fit(trainData.data)
        .thenLabelEstimator(NaiveBayesEstimator(numClasses))
        .fit(trainData.data, trainData.labels)
        .then(MaxClassifier)

    // Evaluate the classifier
    logInfo("Evaluating classifier")

    val testData = NewsgroupsDataLoader(sc, conf.testLocation)
    val testLabels = testData.labels
    val testResults = predictor(testData.data)
    val eval = MulticlassClassifierEvaluator(testResults, testLabels, numClasses)

    logInfo("\n" + eval.summary(NewsgroupsDataLoader.classes))
  }

  case class NewsgroupsConfig(
    trainLocation: String = "",
    testLocation: String = "",
    nGrams: Int = 2,
    commonFeatures: Int = 100000)

  def parse(args: Array[String]): NewsgroupsConfig = new OptionParser[NewsgroupsConfig](appName) {
    head(appName, "0.1")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("testLocation") required() action { (x,c) => c.copy(testLocation=x) }
    opt[Int]("nGrams") action { (x,c) => c.copy(nGrams=x) }
    opt[Int]("commonFeatures") action { (x,c) => c.copy(commonFeatures=x) }
  }.parse(args, NewsgroupsConfig()).get

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   * @param args
   */
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName(appName)
    conf.setIfMissing("spark.master", "local[2]") // This is a fallback if things aren't set via spark submit.

    val sc = new SparkContext(conf)

    val appConfig = parse(args)
    run(sc, appConfig)

    sc.stop()
  }

}
