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
import nodes.util.{ClassLabelIndicatorsFromIntLabels, ZipVectors, TopKClassifier}

import utils.{Image, MatrixUtils, Stats}

object ImageNetSiftLcsFV extends Serializable with Logging {
  val appName = "ImageNetSiftLcsFV"

  def getSiftFeatures(
      conf: ImageNetSiftLcsFVConfig,
      trainParsed: RDD[Image],
      testParsed: RDD[Image]): (RDD[DenseVector[Double]], RDD[DenseVector[Double]]) = {
    // Part 1: Scale and convert images to grayscale.
    val grayscaler = PixelScaler then GrayScaler
    val grayRDD = grayscaler(trainParsed)

    val numImgs = trainParsed.count.toInt
    var siftSamples: Option[RDD[DenseVector[Float]]] = None

    // Part 1a: If necessary, perform PCA on samples of the SIFT features, or load a PCA matrix from
    // disk.
    val pcaTransformer = conf.siftPcaFile match {
      case Some(fname) => new BatchPCATransformer(convert(csvread(new File(fname)), Float).t)
      case None => {
        val pcapipe = new SIFTExtractor(scaleStep = conf.siftScaleStep) then
          new ColumnSampler(conf.numPcaSamples, Some(numImgs))
        siftSamples = Some(pcapipe(grayRDD).cache())
        val pca = new PCAEstimator(conf.descDim).fit(siftSamples.get)

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
        val samples = siftSamples.getOrElse { 
          val siftSampler = new SIFTExtractor(scaleStep = conf.siftScaleStep) then
            new ColumnSampler(conf.numGmmSamples, Some(numImgs))
          siftSampler(grayRDD)
        }
        val vectorPCATransformer = new PCATransformer(pcaTransformer.pcaMat)
        new GaussianMixtureModelEstimator(conf.vocabSize)
          .fit(MatrixUtils.shuffleArray(
            vectorPCATransformer(samples).map(convert(_, Double)).collect()).take(1e6.toInt))
    }

    // Part 3: Compute Fisher Vectors and signed-square-root normalization.
    val fisherFeaturizer =  new FisherVector(gmm) then
        FloatToDouble then
        MatrixVectorizer then
        NormalizeRows then
        SignedHellingerMapper then
        NormalizeRows then
        new Cacher[DenseVector[Double]](Some("sift-fisher"))

    val trainingFeatures = fisherFeaturizer(pcaTransformedRDD)

    val testFeatures = (grayscaler then featurizer then fisherFeaturizer).apply(testParsed)
    (trainingFeatures, testFeatures)
  }

  def getLcsFeatures(
      conf: ImageNetSiftLcsFVConfig,
      trainParsed: RDD[Image],
      testParsed: RDD[Image]): (RDD[DenseVector[Double]], RDD[DenseVector[Double]]) = {

    val numImgs = trainParsed.count.toInt
    var lcsSamples: Option[RDD[DenseVector[Float]]] = None
    // Part 1a: If necessary, perform PCA on samples of the LCS features, or load a PCA matrix from
    // disk.
    val pcaTransformer = conf.lcsPcaFile match {
      case Some(fname) => new BatchPCATransformer(convert(csvread(new File(fname)), Float).t)
      case None => {
        val pcapipe = new LCSExtractor(conf.lcsStride, conf.lcsBorder, conf.lcsPatch) then
          new ColumnSampler(conf.numPcaSamples, Some(numImgs))
        lcsSamples = Some(pcapipe(trainParsed).cache())
        val pca = new PCAEstimator(conf.descDim).fit(lcsSamples.get)

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
        val samples = lcsSamples.getOrElse { 
          val lcsSampler = new LCSExtractor(conf.lcsStride, conf.lcsBorder, conf.lcsPatch) then
            new ColumnSampler(conf.numPcaSamples, Some(numImgs))
          lcsSampler(trainParsed)
        }
        val vectorPCATransformer = new PCATransformer(pcaTransformer.pcaMat)
        new GaussianMixtureModelEstimator(conf.vocabSize)
          .fit(MatrixUtils.shuffleArray(
            vectorPCATransformer(samples).map(convert(_, Double)).collect()).take(1e6.toInt))
    }

    // Part 3: Compute Fisher Vectors and signed-square-root normalization.
    val fisherFeaturizer =  new FisherVector(gmm) then
        FloatToDouble then
        MatrixVectorizer then
        NormalizeRows then
        SignedHellingerMapper then
        NormalizeRows then
        new Cacher[DenseVector[Double]](Some("lcs-fisher"))

    val trainingFeatures = fisherFeaturizer(pcaTransformedRDD)

    val testFeatures = (featurizer then fisherFeaturizer).apply(testParsed)
    (trainingFeatures, testFeatures)
  }

  def run(sc: SparkContext, conf: ImageNetSiftLcsFVConfig) {
    // Load the data and extract training labels.
    val parsedRDD = ImageNetLoader(
      sc,
      conf.trainLocation,
      conf.labelPath).cache().setName("trainData")

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

    val trainParsedImgs = (ImageExtractor).apply(parsedRDD) 
    val testParsedImgs = (ImageExtractor).apply(testParsedRDD)

    // Get SIFT + FV features
    val (trainSift, testSift) = getSiftFeatures(conf, trainParsedImgs, testParsedImgs)

    // Get LCS + FV features
    val (trainLcs, testLcs) = getLcsFeatures(conf, trainParsedImgs, testParsedImgs)

    val trainingFeatures = ZipVectors(Seq(trainSift, trainLcs))
    val testFeatures = ZipVectors(Seq(testSift, testLcs))

    trainingFeatures.count
    val numTestImgs = testFeatures.count

    // Fit a weighted least squares model to the data.
    val model = new BlockWeightedLeastSquaresEstimator(
      4096, 1, conf.lambda, conf.mixtureWeight).fit(
        trainingFeatures, trainingLabels, Some(2 * 2 * conf.descDim * conf.vocabSize))

    // Apply the model to test data and compute test error
    val testPredictedValues = model(testFeatures)
    val testPredicted = TopKClassifier(5).apply(testPredictedValues)

    logInfo("TEST Error is " + Stats.getErrPercent(testPredicted, testActual, numTestImgs) + "%")
  }

  case class ImageNetSiftLcsFVConfig(
    trainLocation: String = "",
    testLocation: String = "",
    labelPath: String = "",
    lambda: Double = 6e-5,
    mixtureWeight: Double = 0.25,
    descDim: Int = 64,
    vocabSize: Int = 16,
    siftScaleStep: Int = 1,
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
    numPcaSamples: Int = 1e7.toInt,
    numGmmSamples: Int = 1e7.toInt)

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
