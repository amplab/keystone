package pipelines.images.cifar

import breeze.linalg.DenseVector
import nodes.CifarLoader
import nodes.images._
import nodes.learning.LinearMapEstimator
import nodes.misc.StandardScaler
import nodes.util.{Cacher, ClassLabelIndicatorsFromIntLabels}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import utils.Stats


object RandomCifar extends Serializable {
  val appName = "RandomCifar"

  def run(sc: SparkContext, conf: RandomCifarConfig) {
    // Set up some constants.
    val numClasses = 10
    val imageSize = 32
    val numChannels = 3

    def dataLoader(fname: String) = CifarLoader(sc, fname)

    // Load up training data, and optionally sample.
    val trainData = conf.sampleFrac match {
      case Some(f) => dataLoader(conf.trainLocation).sample(false, f).cache
      case None => dataLoader(conf.trainLocation).cache
    }

    val trainImages = ImageExtractor(trainData)

    // Set up a filter Array.
    val filters = Stats.randMatrixGaussian(conf.numFilters, conf.patchSize*conf.patchSize*numChannels)

    val featurizer =
      new Convolver(sc, filters, imageSize, imageSize, numChannels, None, true)
        .then(SymmetricRectifier(alpha=conf.alpha))
        .then(new Pooler(conf.poolStride, conf.poolSize, identity, _.sum))
        .then(ImageVectorizer)
        .then(new Cacher[DenseVector[Double]])
        .thenEstimator(new StandardScaler).fit(trainImages)
        .then(new Cacher[DenseVector[Double]])

    val labelExtractor = LabelExtractor then ClassLabelIndicatorsFromIntLabels(numClasses) then new Cacher[DenseVector[Double]]

    val trainFeatures = featurizer(trainImages)
    val trainLabels = labelExtractor(trainData)

    val model = LinearMapEstimator(conf.lambda).fit(trainFeatures, trainLabels)

    val predictionPipeline = featurizer then model then new Cacher[DenseVector[Double]]

    // Calculate training error.
    val trainError = Stats.classificationError(predictionPipeline(trainImages), trainLabels)

    // Do testing.
    val testData = dataLoader(conf.testLocation)
    val testImages = ImageExtractor(testData)
    val testLabels = labelExtractor(testData)

    val testError = Stats.classificationError(predictionPipeline(testImages), testLabels)

    println(s"Training error is: $trainError, Test error is: $testError")


    sc.stop()
    sys.exit(0)
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
    val conf = new SparkConf().setAppName(appName)
    conf.setIfMissing("spark.master", "local[2]")

    val sc = new SparkContext(conf)

    val appConfig = parse(args)
    run(sc, appConfig)

    sc.stop()
  }
}
