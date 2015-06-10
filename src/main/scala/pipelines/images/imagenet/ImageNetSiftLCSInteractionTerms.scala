package pipelines.images.imagenet

import java.io.File
import scala.reflect.ClassTag

import breeze.linalg._
import breeze.stats._
import breeze.stats.distributions._

import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import evaluation.MulticlassClassifierEvaluator
import loaders.ImageNetLoader
import pipelines.Logging

import nodes.images.external.{FisherVector, SIFTExtractor}
import nodes.images._
import nodes.learning._
import nodes.stats.{ColumnSampler, NormalizeRows, SignedHellingerMapper, BatchSignedHellingerMapper}
import nodes.util.{FloatToDouble, MatrixVectorizer, Cacher}
import nodes.util.{ClassLabelIndicatorsFromIntLabels, ZipVectors, TopKClassifier}

import utils.{Image, MatrixUtils, Stats}

object ImageNetSiftLcsInteractionTerms extends Serializable with Logging {
  val appName = "ImageNetSiftLcsInteractionTerms"

  def makeDoubleArrayCsv[T: ClassTag](filenames: RDD[String], data: RDD[Array[T]]): RDD[String] = {
    filenames.zip(data).map { case (x,y) => x + ","+ y.mkString(",") }
  }

  def constructTemplateInteractions(
      samples: DenseMatrix[Double],
      conf: ImageNetSiftLcsInteractionTermsConfig,
      name: Option[String] = None) = {
    // Compute ZCA on samples
    val zcaWhitener = new ZCAWhitenerEstimator().fitSingle(samples)

    // Apply ZCA to sift samples
    // siftSamplesFiltered is 1M by 128
    // 1M * 128
    val whiteSamples = zcaWhitener.apply(samples)
    val normalizedWhiteSamples = Stats.normalizeRows(whiteSamples, 1e-6)
    
    // Now run KMeans++ to get a model
    val kMeans = new KMeansPlusPlusEstimator(conf.numKMeans, 100,
      1e-3).fit(normalizedWhiteSamples)

    // Generate a random gaussian matrix
    val randomGaussianMatrix = DenseMatrix.rand(
      normalizedWhiteSamples.cols, conf.numGaussianRandomFeatures, Rand.gaussian) 
    randomGaussianMatrix :/= math.sqrt(normalizedWhiteSamples.cols)

    val kitchenSinksGaussianWeights = zcaWhitener.whitener * randomGaussianMatrix
    val kitchenSinksGaussianBias = kitchenSinksGaussianWeights.t * zcaWhitener.means

    val kitchenSinksTemplateWeights = zcaWhitener.whitener * kMeans.means.t
    val kitchenSinksTemplateBias = kitchenSinksTemplateWeights.t * zcaWhitener.means 

    // TODO: Make 0.25 a command line argument ?
    val templateInteractions = new TemplateInteractions(
      (kitchenSinksGaussianWeights, kitchenSinksGaussianBias),
      (kitchenSinksTemplateWeights, kitchenSinksTemplateBias),
      0.0,
      0.25)

    // Now we are going to featurize
    val kitchenSinkFeaturizer = {
      templateInteractions then
      MatrixVectorizer then
      SignedHellingerMapper then
      NormalizeRows then
      new Cacher[DenseVector[Double]](name)
    }

    kitchenSinkFeaturizer
  }

  def getSiftFeatures(
      conf: ImageNetSiftLcsInteractionTermsConfig,
      trainParsed: RDD[Image],
      testParsed: RDD[Image]): (RDD[DenseVector[Double]], RDD[DenseVector[Double]]) = {
    // Part 1: Scale and convert images to grayscale.
    val grayscaler = PixelScaler then GrayScaler
    val numImgs = trainParsed.count.toInt

    // TODO: create an element wise apply function node.
    // For functions like sqrt, max, square, pow etc.
    val siftHellinger = (new SIFTExtractor(scaleStep = conf.siftScaleStep) then
      BatchSignedHellingerMapper)

    // Part 1: Get some SIFT samples and then compute the ZCA whitening matrix
    val siftFeatures = {
      grayscaler then 
      siftHellinger
    }.apply(trainParsed)

    val siftSamples = new ColumnSampler(conf.numZcaSamples, Some(numImgs)).apply(
      siftFeatures)

    val siftSamplesFiltered = MatrixUtils.rowsToMatrix(
      siftSamples.filter { sift =>
        norm(sift, 2) > conf.siftThreshold
      }.map { x =>
        convert(x, Double)
      }.collect()
    )

    val featurizedKitchenSink = {
      grayscaler then
      siftHellinger then
      FloatToDouble then
      constructTemplateInteractions(siftSamplesFiltered, conf, Some("sift-features"))
    }
    val trainingFeatures = featurizedKitchenSink(trainParsed)
    val testFeatures = featurizedKitchenSink(testParsed)

    (trainingFeatures, testFeatures)
  }

  def getLcsFeatures(
      conf: ImageNetSiftLcsInteractionTermsConfig,
      trainParsed: RDD[Image],
      testParsed: RDD[Image]): (RDD[DenseVector[Double]], RDD[DenseVector[Double]]) = {
    val numImgs = trainParsed.count.toInt
    val lcsFeatures = {
      new LCSExtractor(conf.lcsStride, conf.lcsBorder, conf.lcsPatch)
    }.apply(trainParsed)
    val lcsSamples = new ColumnSampler(conf.numZcaSamples, Some(numImgs)).apply(
      lcsFeatures)

    val lcsSamplesMat = MatrixUtils.rowsToMatrix(
      lcsSamples.collect().map(x => convert(x, Double)))

    val featurizedKitchenSink = {
      new LCSExtractor(conf.lcsStride, conf.lcsBorder, conf.lcsPatch) then
      FloatToDouble then
      constructTemplateInteractions(lcsSamplesMat, conf, Some("lcs-features"))
    }
    val trainingFeatures = featurizedKitchenSink(trainParsed)
    val testFeatures = featurizedKitchenSink(testParsed)
    (trainingFeatures, testFeatures)
  }

  def run(sc: SparkContext, conf: ImageNetSiftLcsInteractionTermsConfig) {
    // Load the data and extract training labels.
    val parsedRDD = ImageNetLoader(
      sc,
      conf.trainLocation,
      conf.labelPath).cache().setName("trainData")

    val filenamesRDD = parsedRDD.map(_.filename.get)

    val labelGrabber = LabelExtractor then
      ClassLabelIndicatorsFromIntLabels(ImageNetLoader.NUM_CLASSES) then
      new Cacher[DenseVector[Double]]
    val trainingLabels = labelGrabber(parsedRDD)
    trainingLabels.count

    // Load test data and get actual labels
    val testParsedRDD = ImageNetLoader(
      sc,
      conf.testLocation,
      conf.labelPath).cache().setName("testData")
    val testActual = (labelGrabber then TopKClassifier(1)).apply(testParsedRDD)

    val testFilenamesRDD = testParsedRDD.map(_.filename.get)

    val trainParsedImgs = (ImageExtractor).apply(parsedRDD) 
    val testParsedImgs = (ImageExtractor).apply(testParsedRDD)

    // Get SIFT + FV features
    val (trainSift, testSift) = getSiftFeatures(conf, trainParsedImgs, testParsedImgs)

    // Get LCS + FV features
    val (trainLcs, testLcs) = getLcsFeatures(conf, trainParsedImgs, testParsedImgs)

    val trainingFeatures = ZipVectors(Seq(trainSift, trainLcs))
    val testFeatures = ZipVectors(Seq(testSift, testLcs))

    conf.featuresSaveDir.foreach { dir =>
      makeDoubleArrayCsv(filenamesRDD, trainingFeatures.map(_.toArray)).saveAsTextFile(dir + "/featuresTrain")
      makeDoubleArrayCsv(testFilenamesRDD, testFeatures.map(_.toArray)).saveAsTextFile(dir + "/featuresTest")
      makeDoubleArrayCsv(filenamesRDD, trainingLabels.map(_.toArray)).saveAsTextFile(dir + "/trainLabels")
      makeDoubleArrayCsv(testFilenamesRDD, testActual).saveAsTextFile(dir + "/testActual")
    }

    trainingFeatures.count
    val numTestImgs = testFeatures.count

    // Fit a weighted least squares model to the data.
    val model = new BlockWeightedLeastSquaresEstimator(
      4096, 1, conf.lambda, conf.mixtureWeight).fit(
        trainingFeatures, trainingLabels, Some(2 * conf.numKMeans * conf.numGaussianRandomFeatures))

    // Apply the model to test data and compute test error
    val testPredictedValues = model(testFeatures)
    val testPredicted = TopKClassifier(5).apply(testPredictedValues)

    logInfo("TEST Error is " + Stats.getErrPercent(testPredicted, testActual, numTestImgs) + "%")
  }

  case class ImageNetSiftLcsInteractionTermsConfig(
    trainLocation: String = "",
    testLocation: String = "",
    labelPath: String = "",
    lambda: Double = 6e-5,
    mixtureWeight: Double = 0.25,
    numGaussianRandomFeatures: Int = 160,
    numKMeans: Int = 256,
    siftScaleStep: Int = 1,
    lcsStride: Int = 4,
    lcsBorder: Int = 16,
    lcsPatch: Int = 6,
    numZcaSamples: Int = 1e7.toInt,
    siftThreshold: Double = 1e-3.toDouble,
    featuresSaveDir: Option[String] = None)

  def parse(args: Array[String]): ImageNetSiftLcsInteractionTermsConfig = {
    new OptionParser[ImageNetSiftLcsInteractionTermsConfig](appName) {
      head(appName, "0.1")
      help("help") text("prints this usage text")
      opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
      opt[String]("testLocation") required() action { (x,c) => c.copy(testLocation=x) }
      opt[String]("labelPath") required() action { (x,c) => c.copy(labelPath=x) }

      // Solver params
      opt[Double]("lambda") action { (x,c) => c.copy(lambda=x) }
      opt[Double]("mixtureWeight") action { (x,c) => c.copy(mixtureWeight=x) }

      // PCA, GMM params
      opt[Int]("numGaussianRandomFeatures") action { (x,c) => c.copy(numGaussianRandomFeatures=x) }
      opt[Int]("numKMeans") action { (x,c) => c.copy(numKMeans=x) }
      opt[Int]("numZcaSamples") action { (x,c) => c.copy(numZcaSamples=x) }
      opt[Double]("siftThreshold") action { (x,c) => c.copy(siftThreshold=x) }

      // SIFT, LCS params
      opt[Int]("siftScaleStep") action { (x,c) => c.copy(siftScaleStep=x) }
      opt[Int]("lcsStride") action { (x,c) => c.copy(lcsStride=x) }
      opt[Int]("lcsBorder") action { (x,c) => c.copy(lcsBorder=x) }
      opt[Int]("lcsPatch") action { (x,c) => c.copy(lcsPatch=x) }

      opt[String]("featuresSaveDir") action { (x, c) => c.copy(featuresSaveDir=Some(x)) }
    }.parse(args, ImageNetSiftLcsInteractionTermsConfig()).get
  }

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
