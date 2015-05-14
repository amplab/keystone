package pipelines.images.mnist

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats.distributions.{ThreadLocalRandomGenerator, RandBasis, Rand}
import evaluation.MulticlassClassifierEvaluator
import loaders.{CsvDataLoader, LabeledData}
import nodes._
import nodes.images._
import nodes.learning.{BlockLinearMapper, LinearMapEstimator}
import nodes.misc.{PaddedFFT, StandardScaler}
import nodes.stats.RandomSignNode
import nodes.util.{Cacher, ClassLabelIndicatorsFromIntLabels, MaxClassifier}
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import pipelines._
import scopt.OptionParser
import utils.{Stats, ImageMetadata}


object MnistRandomFFT extends Serializable with Logging {
  val appName = "MnistRandomFFT"

  def zipRdds(in: Seq[RDD[DenseVector[Double]]]) = in.reduceLeft((a,b) => a.zip(b).map(r => DenseVector.vertcat(r._1, r._2)))

  def run(sc: SparkContext, conf: MnistRandomFFTConfig) {
    // Set up some constants.
    val numParts = 10
    val numClasses = 10

    val seed = 0L
    val random = new java.util.Random(seed)
    val randomSignSource = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(random.nextLong())))

    val d = 784
    val fftsPerBatch = 8
    val numFFTBatches = conf.numFFTs/fftsPerBatch

    val startTime = System.currentTimeMillis

    val train = LabeledData(
      CsvDataLoader(sc, conf.trainLocation, numParts)
        .map(x => (x(0).toInt, x(1 until x.length)))
        .cache())
    val labels = ClassLabelIndicatorsFromIntLabels(numClasses).apply(train.labels)

    val batchFeaturizer = (0 until numFFTBatches).map { batch =>
      (0 until fftsPerBatch).map { x =>
        RandomSignNode(d, randomSignSource).then(PaddedFFT).then(LinearRectifier(0.0))
      }
    }

    val trainingBatches = batchFeaturizer.map { x =>
      zipRdds(x.map(y => y.apply(train.data))).cache()
    }

    // Train the model
    val blockLinearMapper = BlockLinearMapper.trainWithL2(trainingBatches, labels, conf.lambda.getOrElse(0), 1)

    val test = LabeledData(
      CsvDataLoader(sc, conf.testLocation, numParts)
          .map(x => (x(0).toInt, x(1 until x.length)))
          .cache())
    val actual = test.labels

    val testBatches = batchFeaturizer.toIterator.map { x =>
      zipRdds(x.map(y => y.apply(test.data))).cache()
    }

    // Calculate test error
    blockLinearMapper.applyAndEvaluate(testBatches, testPredictedValues => {
      val predicted = MaxClassifier(testPredictedValues)
      val evaluator = MulticlassClassifierEvaluator(predicted, actual, numClasses)
      println("TEST Error is " + (100d - 100d * evaluator.microAccuracy) + "%")
    })

    val endTime = System.currentTimeMillis
    logInfo("Pipeline took " + (endTime - startTime)/1000 + " s")
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
