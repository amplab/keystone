package pipelines.speech

import breeze.stats.distributions.{RandBasis, ThreadLocalRandomGenerator}
import evaluation.MulticlassClassifierEvaluator
import loaders.{TimitFeaturesDataLoader, CsvDataLoader}
import nodes.learning.BlockLinearMapper
import nodes.misc.{CosineRandomFeatures, StandardScaler}
import nodes.util.{MaxClassifier, ClassLabelIndicatorsFromIntLabels}
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import pipelines._
import scopt.OptionParser

import scala.collection.mutable


object TimitPipeline extends Logging {
  val appName = "Timit"

  case class TimitConfig(
    trainDataLocation: String = "",
    trainLabelsLocation: String = "",
    testDataLocation: String = "",
    testLabelsLocation: String = "",
    numParts: Int = 512,
    numCosines: Int = 50,
    gamma: Double = 0.05555,
    rfType: String = "gaussian",
    lambda: Double = 0.0,
    numEpochs: Int = 5,
    checkpointDir: Option[String] = None)

  def run(sc: SparkContext, conf: TimitConfig) {

    conf.checkpointDir.foreach(_ => sc.setCheckpointDir(_))

    Thread.sleep(5000)

    // Set the constants
    val seed = 123L
    val random = new java.util.Random(seed)
    val randomSignSource = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(random.nextLong())))

    val numCosineFeatures = 4096
    val numCosineBatches = conf.numCosines
    val colsPerBatch = numCosineFeatures + 1

    // Load the data
    val timitFeaturesData = TimitFeaturesDataLoader(
      sc,
      conf.trainDataLocation,
      conf.trainLabelsLocation,
      conf.testDataLocation,
      conf.testLabelsLocation,
      conf.numParts)

    // Build the pipeline
    val trainData = timitFeaturesData.train.data.cache().setName("trainRaw")
    trainData.count()

    val batchFeaturizer = (0 until numCosineBatches).map { batch =>
      if (conf.rfType == "cauchy") {
        CosineRandomFeatures.createCauchyCosineRF(
          TimitFeaturesDataLoader.timitDimension,
          numCosineFeatures,
          conf.gamma,
          randomSignSource)
      } else {
        CosineRandomFeatures.createGaussianCosineRF(
          TimitFeaturesDataLoader.timitDimension,
          numCosineFeatures,
          conf.gamma,
          randomSignSource)
      }.thenEstimator(new StandardScaler()).fit(trainData)
    }

    val trainingBatches = batchFeaturizer.map { x =>
      x.apply(trainData)
    }

    val labels = ClassLabelIndicatorsFromIntLabels(TimitFeaturesDataLoader.numClasses).apply(
      timitFeaturesData.train.labels
    ).cache().setName("trainLabels")

    val testData = timitFeaturesData.test.data.cache().setName("testRaw")
    val numTest = testData.count()

    val testBatches = batchFeaturizer.map { case x =>
      val rdd = x.apply(testData)
      rdd.cache().setName("testFeatures")
    }

    val actual = timitFeaturesData.test.labels.cache().setName("actual")

    // Train the model
    val blockLinearMapper = BlockLinearMapper.trainWithL2(trainingBatches, labels, conf.lambda, conf.numEpochs)

    // Calculate test error
    blockLinearMapper.applyAndEvaluate(testBatches, testPredictedValues => {
      val predicted = MaxClassifier(testPredictedValues)
      val evaluator = MulticlassClassifierEvaluator(predicted, actual, TimitFeaturesDataLoader.numClasses)
      println("TEST Error is " + (100d - 100d * evaluator.microAccuracy) + "%")
    })

    System.exit(0)
  }

  def parse(args: Array[String]): TimitConfig = new OptionParser[TimitConfig](appName) {
    head(appName, "0.1")
    opt[String]("trainDataLocation") required() action { (x,c) => c.copy(trainDataLocation=x) }
    opt[String]("trainLabelsLocation") required() action { (x,c) => c.copy(trainLabelsLocation=x) }
    opt[String]("testDataLocation") required() action { (x,c) => c.copy(testDataLocation=x) }
    opt[String]("testLabelsLocation") required() action { (x,c) => c.copy(testLabelsLocation=x) }
    opt[String]("checkpointDir") action { (x,c) => c.copy(checkpointDir=Some(x)) }
    opt[String]("rfType") action { (x,c) => c.copy(rfType=x) }
    opt[Int]("numParts") action { (x,c) => c.copy(numParts=x) }
    opt[Int]("numCosines") action { (x,c) => c.copy(numCosines=x) }
    opt[Int]("numEpochs") action { (x,c) => c.copy(numEpochs=x) }
    opt[Double]("gamma") action { (x,c) => c.copy(gamma=x) }
    opt[Double]("lambda") action { (x,c) => c.copy(lambda=x) }
  }.parse(args, TimitConfig()).get

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
