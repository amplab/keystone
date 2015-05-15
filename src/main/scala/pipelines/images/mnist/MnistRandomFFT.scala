package pipelines.images.mnist

import breeze.linalg.DenseVector
import breeze.stats.distributions.{RandBasis, ThreadLocalRandomGenerator}
import evaluation.MulticlassClassifierEvaluator
import loaders.{CsvDataLoader, LabeledData}
import nodes.images._
import nodes.learning.BlockLinearMapper
import nodes.misc.ZipRDDs
import nodes.stats.{LinearRectifier, PaddedFFT, RandomSignNode}
import nodes.util.{ClassLabelIndicatorsFromIntLabels, MaxClassifier}
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import pipelines._
import scopt.OptionParser


object MnistRandomFFT extends Serializable with Logging {
  val appName = "MnistRandomFFT"

  def run(sc: SparkContext, conf: MnistRandomFFTConfig) {
    // Set up some constants.
    val numParts = 10
    val numClasses = 10

    val seed = 0L
    val random = new java.util.Random(seed)
    val randomSignSource = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(random.nextLong())))

    val d = 784
    // Because d is 784, we get 512 PaddedFFT features per FFT.
    // So, setting fftsPerBatch to 8 leads to a block size of 4096 features.
    val fftsPerBatch = 8
    val numFFTBatches = conf.numFFTs/fftsPerBatch

    val startTime = System.nanoTime()

    val train = LabeledData(
      CsvDataLoader(sc, conf.trainLocation, numParts)
        .map(x => (x(0).toInt - 1, x(1 until x.length)))
        .cache())
    val labels = ClassLabelIndicatorsFromIntLabels(numClasses).apply(train.labels)

    val batchFeaturizer = (0 until numFFTBatches).map { batch =>
      (0 until fftsPerBatch).map { x =>
        RandomSignNode(d, randomSignSource).then(PaddedFFT).then(LinearRectifier(0.0))
      }
    }

    val trainingBatches = batchFeaturizer.map { x =>
      ZipRDDs(x.map(y => y.apply(train.data))).cache()
    }

    // Train the model
    val blockLinearMapper = BlockLinearMapper.trainWithL2(trainingBatches, labels, conf.lambda.getOrElse(0), 1)

    val test = LabeledData(
      CsvDataLoader(sc, conf.testLocation, numParts)
          .map(x => (x(0).toInt - 1, x(1 until x.length)))
          .cache())
    val actual = test.labels

    val testBatches = batchFeaturizer.toIterator.map { x =>
      ZipRDDs(x.map(y => y.apply(test.data))).cache()
    }

    // Calculate train error
    blockLinearMapper.applyAndEvaluate(trainingBatches, trainPredictedValues => {
      val predicted = MaxClassifier(trainPredictedValues)
      val evaluator = MulticlassClassifierEvaluator(predicted, train.labels, numClasses)
      logInfo("Train Error is " + (100 * evaluator.totalError) + "%")
    })

    // Calculate test error
    blockLinearMapper.applyAndEvaluate(testBatches, testPredictedValues => {
      val predicted = MaxClassifier(testPredictedValues)
      val evaluator = MulticlassClassifierEvaluator(predicted, actual, numClasses)
      logInfo("TEST Error is " + (100 * evaluator.totalError) + "%")
    })

    val endTime = System.nanoTime()
    logInfo("Pipeline took " + (endTime - startTime)/1e9 + " s")
  }

  case class MnistRandomFFTConfig(
      trainLocation: String = "",
      testLocation: String = "",
      numFFTs: Int = 200,
      lambda: Option[Double] = None)

  def parse(args: Array[String]): MnistRandomFFTConfig = new OptionParser[MnistRandomFFTConfig](appName) {
    head(appName, "0.1")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("testLocation") required() action { (x,c) => c.copy(testLocation=x) }
    opt[Int]("numFFTs") action { (x,c) => c.copy(numFFTs=x) }
    opt[Double]("lambda") action { (x,c) => c.copy(lambda=Some(x)) }
  }.parse(args, MnistRandomFFTConfig()).get

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
