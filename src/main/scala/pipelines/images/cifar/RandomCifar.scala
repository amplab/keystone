package pipelines.images.cifar

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats.distributions.Rand
import evaluation.MulticlassClassifierEvaluator
import loaders.CifarLoader
import nodes.images._
import nodes.learning.LinearMapEstimator
import nodes.stats.StandardScaler
import nodes.util.{Cacher, ClassLabelIndicatorsFromIntLabels, MaxClassifier}
import org.apache.spark.{SparkConf, SparkContext}
import pipelines.Logging
import scopt.OptionParser


object RandomCifar extends Serializable with Logging {
  val appName = "RandomCifar"

  def run(sc: SparkContext, conf: RandomCifarConfig) {
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

    // Set up a filter Array.
    val filters = DenseMatrix.rand(conf.numFilters, conf.patchSize*conf.patchSize*numChannels, Rand.gaussian)

    val featurizer =
      new Convolver(filters, imageSize, imageSize, numChannels, None, true)
        .then(SymmetricRectifier(alpha=conf.alpha))
        .then(new Pooler(conf.poolStride, conf.poolSize, identity, _.sum))
        .then(ImageVectorizer)
        .then(new Cacher[DenseVector[Double]])
        .thenEstimator(new StandardScaler).withData(trainImages)
        .then(new Cacher[DenseVector[Double]]).fit()

    val labelExtractor = LabelExtractor then
      ClassLabelIndicatorsFromIntLabels(numClasses) then
      new Cacher[DenseVector[Double]]

    val trainFeatures = featurizer(trainImages)
    val trainLabels = labelExtractor(trainData)

    val model = LinearMapEstimator(conf.lambda).withData(trainFeatures, trainLabels).fit()

    val predictionPipeline = featurizer then model then MaxClassifier

    // Calculate training error.
    val trainEval = MulticlassClassifierEvaluator(
      predictionPipeline(trainImages), LabelExtractor(trainData), numClasses)

    // Do testing.
    val testData = CifarLoader(sc, conf.testLocation)
    val testImages = ImageExtractor(testData)
    val testLabels = labelExtractor(testData)

    val testEval = MulticlassClassifierEvaluator(predictionPipeline(testImages), LabelExtractor(testData), numClasses)

    logInfo(s"Training error is: ${trainEval.totalError}")
    logInfo(s"Test error is: ${testEval.totalError}")
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
