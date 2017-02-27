package pipelines.text

import breeze.linalg.SparseVector
import evaluation.BinaryClassifierEvaluator
import loaders.{AmazonReviewsDataLoader, LabeledData}
import nodes.learning.LogisticRegressionEstimator
import nodes.nlp._
import nodes.stats.TermFrequency
import nodes.util.CommonSparseFeatures
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import pipelines.Logging
import scopt.OptionParser
import workflow.Pipeline

object AmazonReviewsPipeline extends Logging {
  val appName = "AmazonReviewsPipeline"

  def run(spark: SparkSession, conf: AmazonReviewsConfig): Pipeline[String, Double] = {
    val amazonTrainData = AmazonReviewsDataLoader(spark, conf.trainLocation, conf.threshold).labeledData
    val trainData = LabeledData(amazonTrainData.repartition(conf.numParts).cache())

    val training = trainData.data
    val labels = trainData.labels

    // Build the classifier estimator
    val predictor = Trim andThen
        LowerCase() andThen
        Tokenizer() andThen
        NGramsFeaturizer(1 to conf.nGrams) andThen
        TermFrequency(x => 1) andThen
        (CommonSparseFeatures[Seq[String]](conf.commonFeatures), training) andThen
        (LogisticRegressionEstimator[SparseVector[Double]](numClasses = 2, numIters = conf.numIters), training, labels)

    // Evaluate the classifier
    val amazonTestData = AmazonReviewsDataLoader(spark, conf.testLocation, conf.threshold).labeledData
    val testData = LabeledData(amazonTestData.repartition(conf.numParts).cache())
    val testLabels = testData.labels
    val testResults = predictor(testData.data)
    val eval = BinaryClassifierEvaluator.evaluate(testResults.get.map(_ > 0), testLabels.map(_ > 0))

    logInfo("\n" + eval.summary())
    predictor
  }

  case class AmazonReviewsConfig(
    trainLocation: String = "",
    testLocation: String = "",
    threshold: Double = 3.5,
    nGrams: Int = 2,
    commonFeatures: Int = 100000,
    numIters: Int = 20,
    numParts: Int = 512)

  def parse(args: Array[String]): AmazonReviewsConfig = new OptionParser[AmazonReviewsConfig](appName) {
    head(appName, "0.1")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("testLocation") required() action { (x,c) => c.copy(testLocation=x) }
    opt[Double]("threshold") action { (x,c) => c.copy(threshold=x)}
    opt[Int]("nGrams") action { (x,c) => c.copy(nGrams=x) }
    opt[Int]("commonFeatures") action { (x,c) => c.copy(commonFeatures=x) }
    opt[Int]("numIters") action { (x,c) => c.copy(numParts=x) }
    opt[Int]("numParts") action { (x,c) => c.copy(numParts=x) }
  }.parse(args, AmazonReviewsConfig()).get

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   *
   * @param args
   */
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName(appName)
    conf.setIfMissing("spark.master", "local[2]") // This is a fallback if things aren't set via spark submit.

    val spark = SparkSession.builder.config(conf).getOrCreate()

    val appConfig = parse(args)
    run(spark, appConfig)

    spark.stop()
  }
}
