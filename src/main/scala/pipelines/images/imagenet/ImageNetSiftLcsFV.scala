package pipelines.images.imagenet

import java.io.File

import breeze.linalg._
import breeze.stats._

import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import evaluation.MulticlassClassifierEvaluator
import loaders.ImageNetLoader
import pipelines.Logging

import nodes.images.external.{FisherVector, SIFTExtractor}
import nodes.images._
import nodes.learning._
import nodes.stats.{ColumnSampler, NormalizeRows, SignedHellingerMapper}
import nodes.util.{FloatToDouble, MatrixVectorizer, Cacher}
import nodes.util.{ClassLabelIndicatorsFromIntLabels, ZipVectors, MaxClassifier}

import utils.Image

object ImageNetSiftLcsFV extends Serializable with Logging {
  val appName = "ImageNetSiftLcsFV"

  def getSiftFeatures(
      conf: ImageNetSiftLcsFVConfig,
      trainParsed: RDD[Image],
      testParsed: RDD[Image]): (RDD[DenseVector[Double]], RDD[DenseVector[Double]]) = {
    // Part 1: Scale and convert images to grayscale.
    val grayscaler = PixelScaler then GrayScaler
    val grayRDD = grayscaler(trainParsed)

    // Part 1a: If necessary, perform PCA on samples of the SIFT features, or load a PCA matrix from
    // disk.
    val pcaTransformer = conf.siftPcaFile match {
      case Some(fname) => new BatchPCATransformer(convert(csvread(new File(fname)), Float).t)
      case None => {
        val pcapipe = new SIFTExtractor(scaleStep = conf.siftScaleStep) then
          new ColumnSampler(conf.numPcaSamples)
        val pca = new PCAEstimator(conf.descDim).fit(pcapipe(grayRDD))

        new BatchPCATransformer(pca.pcaMat)
      }
    }

    // Part 2: Compute dimensionality-reduced PCA features.
    val featurizer =  new SIFTExtractor(scaleStep = conf.siftScaleStep) then
      pcaTransformer
    val pcaTransformedRDD = featurizer(grayRDD)

    // Part 2a: If necessary, compute a GMM based on the dimensionality-reduced features, or load
    // from disk.
    val gmm = conf.siftGmmMeanFile match {
      case Some(f) =>
        new GaussianMixtureModel(
          csvread(new File(conf.siftGmmMeanFile.get)),
          csvread(new File(conf.siftGmmVarFile.get)),
          csvread(new File(conf.siftGmmWtsFile.get)).toDenseVector)
      case None =>
        val sampler = new ColumnSampler(conf.numGmmSamples)
        new GaussianMixtureModelEstimator(conf.vocabSize)
          .fit(sampler(pcaTransformedRDD).map(convert(_, Double)))
    }

    // Part 3: Compute Fisher Vectors and signed-square-root normalization.
    val fisherFeaturizer =  new FisherVector(gmm) then
        FloatToDouble then
        MatrixVectorizer then
        NormalizeRows then
        SignedHellingerMapper then
        NormalizeRows then
        new Cacher[DenseVector[Double]]

    val trainingFeatures = fisherFeaturizer(pcaTransformedRDD)

    val testFeatures = (grayscaler then featurizer then fisherFeaturizer).apply(testParsed)
    (trainingFeatures, testFeatures)
  }

  def getLcsFeatures(
      conf: ImageNetSiftLcsFVConfig,
      trainParsed: RDD[Image],
      testParsed: RDD[Image]): (RDD[DenseVector[Double]], RDD[DenseVector[Double]]) = {
    // Part 1a: If necessary, perform PCA on samples of the LCS features, or load a PCA matrix from
    // disk.
    val pcaTransformer = conf.lcsPcaFile match {
      case Some(fname) => new BatchPCATransformer(convert(csvread(new File(fname)), Float).t)
      case None => {
        val pcapipe = new LCSExtractor(conf.lcsStride, conf.lcsBorder, conf.lcsPatch) then
          new ColumnSampler(conf.numPcaSamples)
        val pca = new PCAEstimator(conf.descDim).fit(pcapipe(trainParsed))

        new BatchPCATransformer(pca.pcaMat)
      }
    }

    // Part 2: Compute dimensionality-reduced PCA features.
    val featurizer =  new LCSExtractor(conf.lcsStride, conf.lcsBorder, conf.lcsPatch) then
      pcaTransformer
    val pcaTransformedRDD = featurizer(trainParsed)

    // Part 2a: If necessary, compute a GMM based on the dimensionality-reduced features, or load
    // from disk.
    val gmm = conf.lcsGmmMeanFile match {
      case Some(f) =>
        new GaussianMixtureModel(
          csvread(new File(conf.lcsGmmMeanFile.get)),
          csvread(new File(conf.lcsGmmVarFile.get)),
          csvread(new File(conf.lcsGmmWtsFile.get)).toDenseVector)
      case None =>
        val sampler = new ColumnSampler(conf.numGmmSamples)
        new GaussianMixtureModelEstimator(conf.vocabSize)
          .fit(sampler(pcaTransformedRDD).map(convert(_, Double)))
    }

    // Part 3: Compute Fisher Vectors and signed-square-root normalization.
    val fisherFeaturizer =  new FisherVector(gmm) then
        FloatToDouble then
        MatrixVectorizer then
        NormalizeRows then
        SignedHellingerMapper then
        NormalizeRows then
        new Cacher[DenseVector[Double]]

    val trainingFeatures = fisherFeaturizer(pcaTransformedRDD)

    val testFeatures = (featurizer then fisherFeaturizer).apply(testParsed)
    (trainingFeatures, testFeatures)
  }

  def run(sc: SparkContext, conf: ImageNetSiftLcsFVConfig) {
    // Load the data and extract training labels.
    val parsedRDD = ImageNetLoader(
      sc,
      conf.trainLocation,
      conf.labelPath)

    val labelGrabber = LabelExtractor then
      ClassLabelIndicatorsFromIntLabels(ImageNetLoader.NUM_CLASSES) then
      new Cacher[DenseVector[Double]]
    val trainingLabels = labelGrabber(parsedRDD)

    // Load test data and get actual labels
    val testParsedRDD = ImageNetLoader(
      sc,
      conf.testLocation,
      conf.labelPath)
    val testActual = (LabelExtractor then new Cacher[Int]).apply(testParsedRDD)

    // Get SIFT + FV features
    val (trainSift, testSift) = getSiftFeatures(conf, ImageExtractor(parsedRDD),
      ImageExtractor(testParsedRDD))

    // Get LCS + FV features
    val (trainLcs, testLcs) = getLcsFeatures(conf, ImageExtractor(parsedRDD),
      ImageExtractor(testParsedRDD))

    val trainingFeatures = ZipVectors(Seq(trainSift, trainLcs))
    val testFeatures = ZipVectors(Seq(testSift, testLcs))

    // Fit a weighted least squares model to the data.
    val model = new BlockWeightedLeastSquaresEstimator(
      4096, 1, conf.lambda, conf.mixtureWeight).fit(trainingFeatures, trainingLabels)

    // Apply the model to test data and compute test error
    val testPredictedValues = model(testFeatures)
    val testPredicted = MaxClassifier(testPredictedValues)
    val evaluator = MulticlassClassifierEvaluator(testPredicted, testActual,
      ImageNetLoader.NUM_CLASSES)

    logInfo("TEST Error is " + (100.0 * evaluator.totalError) + "%")
  }

  case class ImageNetSiftLcsFVConfig(
    trainLocation: String = "",
    testLocation: String = "",
    labelPath: String = "",
    lambda: Double = 1e-4,
    mixtureWeight: Double = 0.25,
    descDim: Int = 64,
    vocabSize: Int = 16,
    siftScaleStep: Int = 0,
    lcsStride: Int = 4,
    lcsBorder: Int = 16,
    lcsPatch: Int = 6,
    siftPcaFile: Option[String] = None,
    siftGmmMeanFile: Option[String]= None,
    siftGmmVarFile: Option[String] = None,
    siftGmmWtsFile: Option[String] = None,
    lcsPcaFile: Option[String] = None,
    lcsGmmMeanFile: Option[String]= None,
    lcsGmmVarFile: Option[String] = None,
    lcsGmmWtsFile: Option[String] = None,
    numPcaSamples: Int = 1e6.toInt,
    numGmmSamples: Int = 1e6.toInt)

  def parse(args: Array[String]): ImageNetSiftLcsFVConfig = {
    new OptionParser[ImageNetSiftLcsFVConfig](appName) {
      head(appName, "0.1")
      help("help") text("prints this usage text")
      opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
      opt[String]("testLocation") required() action { (x,c) => c.copy(testLocation=x) }
      opt[String]("labelPath") required() action { (x,c) => c.copy(labelPath=x) }

      // Solver params
      opt[Double]("lambda") action { (x,c) => c.copy(lambda=x) }
      opt[Double]("mixtureWeight") action { (x,c) => c.copy(mixtureWeight=x) }

      // PCA, GMM params
      opt[Int]("descDim") action { (x,c) => c.copy(descDim=x) }
      opt[Int]("vocabSize") action { (x,c) => c.copy(vocabSize=x) }
      opt[Int]("numPcaSamples") action { (x,c) => c.copy(numPcaSamples=x) }
      opt[Int]("numGmmSamples") action { (x,c) => c.copy(numGmmSamples=x) }

      // SIFT, LCS params
      opt[Int]("siftScaleStep") action { (x,c) => c.copy(siftScaleStep=x) }
      opt[Int]("lcsStride") action { (x,c) => c.copy(lcsStride=x) }
      opt[Int]("lcsBorder") action { (x,c) => c.copy(lcsBorder=x) }
      opt[Int]("lcsPatch") action { (x,c) => c.copy(lcsPatch=x) }

      // Optional file to load stuff from
      opt[String]("siftPcaFile") action { (x,c) => c.copy(siftPcaFile=Some(x)) }
      opt[String]("siftGmmMeanFile") action { (x,c) => c.copy(siftGmmMeanFile=Some(x)) }
      opt[String]("siftGmmVarFile") action { (x,c) => c.copy(siftGmmVarFile=Some(x)) }
      opt[String]("siftGmmWtsFile") action { (x,c) => c.copy(siftGmmWtsFile=Some(x)) }

      opt[String]("lcsPcaFile") action { (x,c) => c.copy(lcsPcaFile=Some(x)) }
      opt[String]("lcsGmmMeanFile") action { (x,c) => c.copy(lcsGmmMeanFile=Some(x)) }
      opt[String]("lcsGmmVarFile") action { (x,c) => c.copy(lcsGmmVarFile=Some(x)) }
      opt[String]("lcsGmmWtsFile") action { (x,c) => c.copy(lcsGmmWtsFile=Some(x)) }
    }.parse(args, ImageNetSiftLcsFVConfig()).get
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
