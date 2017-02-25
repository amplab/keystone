package keystoneml.pipelines.images.cifar

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats.distributions.Rand
import keystoneml.evaluation.MulticlassClassifierEvaluator
import keystoneml.loaders.CifarLoader
import keystoneml.nodes.images._
import keystoneml.nodes.learning.LinearMapEstimator
import keystoneml.nodes.stats.StandardScaler
import keystoneml.nodes.util.{Cacher, ClassLabelIndicatorsFromIntLabels, MaxClassifier}
import org.apache.spark.{SparkConf, SparkContext}
import keystoneml.pipelines.Logging
import scopt.OptionParser
import keystoneml.utils.Image
import keystoneml.workflow.Pipeline


object RandomCifar extends Serializable with Logging {
  val appName = "RandomCifar"

  def run(sc: SparkContext, conf: RandomCifarConfig): Pipeline[Image, Int] = {
    // Set up some constants.
    val numClasses = 10
    val imageSize = 32
    val numChannels = 3

    // Load up training data, and optionally sample.
    val trainData = conf.sampleFrac match {
      case Some(f) => CifarLoader(sc, conf.trainLocation).sample(false, f).cache
      case None => CifarLoader(sc, conf.trainLocation).cache
    }

    val trainImages = ImageExtractor(trainData)

    val labelExtractor = LabelExtractor andThen
      ClassLabelIndicatorsFromIntLabels(numClasses) andThen
      new Cacher[DenseVector[Double]]

    val trainLabels = labelExtractor(trainData)

    // Set up a filter Array.
    val filters = DenseMatrix.rand(conf.numFilters, conf.patchSize*conf.patchSize*numChannels, Rand.gaussian)

    val predictionPipeline = new Convolver(filters, imageSize, imageSize, numChannels, None, true) andThen
        SymmetricRectifier(alpha=conf.alpha) andThen
        new Pooler(conf.poolStride, conf.poolSize, identity, _.sum) andThen
        ImageVectorizer andThen
        new Cacher[DenseVector[Double]] andThen
        (new StandardScaler, trainImages) andThen
        new Cacher[DenseVector[Double]] andThen
        (LinearMapEstimator(conf.lambda), trainImages, trainLabels) andThen
        MaxClassifier

    // Calculate training error.
    val evaluator = new MulticlassClassifierEvaluator(numClasses)
    val trainEval = evaluator(predictionPipeline(trainImages), LabelExtractor(trainData))

    // Do testing.
    val testData = CifarLoader(sc, conf.testLocation)
    val testImages = ImageExtractor(testData)
    val testLabels = labelExtractor(testData)

    val testEval = evaluator(predictionPipeline(testImages), LabelExtractor(testData))

    logInfo(s"Training error is: ${trainEval.totalError}")
    logInfo(s"Test error is: ${testEval.totalError}")

    predictionPipeline
  }

  case class RandomCifarConfig(
      trainLocation: String = "",
      testLocation: String = "",
      numFilters: Int = 100,
      patchSize: Int = 6,
      poolSize: Int = 14,
      poolStride: Int = 13,
      alpha: Double = 0.25,
      lambda: Option[Double] = None,
      sampleFrac: Option[Double] = None)

  def parse(args: Array[String]): RandomCifarConfig = new OptionParser[RandomCifarConfig](appName) {
    head(appName, "0.1")
    help("help") text("prints this usage text")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("testLocation") required() action { (x,c) => c.copy(testLocation=x) }
    opt[Int]("numFilters") action { (x,c) => c.copy(numFilters=x) }
    opt[Int]("patchSize") action { (x,c) => c.copy(patchSize=x) }
    opt[Int]("poolSize") action { (x,c) => c.copy(poolSize=x) }
    opt[Double]("alpha") action { (x,c) => c.copy(alpha=x) }
    opt[Double]("lambda") action { (x,c) => c.copy(lambda=Some(x)) }
    opt[Double]("sampleFrac") action { (x,c) => c.copy(sampleFrac=Some(x)) }
  }.parse(args, RandomCifarConfig()).get

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   *
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
