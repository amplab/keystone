package pipelines.images.imagenet

import java.io.File
import scala.reflect.ClassTag

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
import nodes.stats.{ColumnSampler, NormalizeRows, SignedHellingerMapper, BatchSignedHellingerMapper}
import nodes.util.{FloatToDouble, MatrixVectorizer, Cacher}
import nodes.util.{ClassLabelIndicatorsFromIntLabels, ZipVectors, TopKClassifier}

import utils.{Image, MatrixUtils, Stats}

object LazyImageNetSiftLcsFV extends Serializable with Logging {
  val appName = "LazyImageNetSiftLcsFV"

  def makeDoubleArrayCsv[T: ClassTag](filenames: RDD[String], data: RDD[Array[T]]): RDD[String] = {
    filenames.zip(data).map { case (x,y) => x + ","+ y.mkString(",") }
  }

  // Takes in a GMM and splits it into GMMs with a subset of centroids in each model.
  // Number of output GMMs is (number of centroids in gmm) / centroidBatchSize
  def splitGMMCentroids(
      gmm: GaussianMixtureModel,
      centroidBatchSize: Int): Seq[GaussianMixtureModel] = {
    val totalNumCentroids = gmm.means.cols
    val numBatches = math.ceil(totalNumCentroids.toDouble / centroidBatchSize).toInt
    (0 until numBatches).map { batch =>
      val start = batch * centroidBatchSize
      val end = math.min((batch + 1) * centroidBatchSize, totalNumCentroids)
      GaussianMixtureModel(
        gmm.means(::, start until end),
        gmm.variances(::, start until end),
        gmm.weights(start until end) 
      )
    }
  }

  def constructFisherFeaturizer(gmm: GaussianMixtureModel, name: Option[String] = None) = {
    // Part 3: Compute Fisher Vectors and signed-square-root normalization.
    val fisherFeaturizer =  new FisherVector(gmm) then
        FloatToDouble then
        MatrixVectorizer then
        NormalizeRows then
        SignedHellingerMapper then
        NormalizeRows
    fisherFeaturizer
  }

  def getSiftFeatures(
      conf: LazyImageNetSiftLcsFVConfig,
      trainParsed: RDD[Image],
      testParsed: RDD[Image])
    : (Iterator[RDD[DenseVector[Double]]], Iterator[RDD[DenseVector[Double]]]) = {

    // Part 1: Scale and convert images to grayscale.
    val grayscaler = PixelScaler then GrayScaler
    val grayRDD = grayscaler(trainParsed)

    val numImgs = trainParsed.count.toInt
    var siftSamples: Option[RDD[DenseVector[Float]]] = None

    val siftHellinger = (new SIFTExtractor(scaleStep = conf.siftScaleStep) then
      BatchSignedHellingerMapper)

    // Part 1a: If necessary, perform PCA on samples of the SIFT features, or load a PCA matrix from
    // disk.
    val pcaTransformer = conf.siftPcaFile match {
      case Some(fname) => new BatchPCATransformer(convert(csvread(new File(fname)), Float).t)
      case None => {
        siftSamples = Some(
          new ColumnSampler(conf.numPcaSamples, Some(numImgs)).apply(siftHellinger(grayRDD)).cache())
        val pca = new PCAEstimator(conf.descDim).fit(siftSamples.get)

        new BatchPCATransformer(pca.pcaMat)
      }
    }

    // Part 2: Compute dimensionality-reduced PCA features.
    val featurizer = siftHellinger then pcaTransformer
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
          new ColumnSampler(conf.numGmmSamples, Some(numImgs)).apply(siftHellinger(grayRDD))
        }
        val vectorPCATransformer = new PCATransformer(pcaTransformer.pcaMat)
        new GaussianMixtureModelEstimator(conf.vocabSize)
          .fit(MatrixUtils.shuffleArray(
            vectorPCATransformer(samples).map(convert(_, Double)).collect()).take(1e6.toInt))
    }

    val splitGMMs = splitGMMCentroids(gmm, conf.centroidBatchSize)

    // TODO(shivaram): Is it okay to create fisher featurizer part of the pipeline twice ??

    val trainingFeatures = splitGMMs.iterator.map { gmmPart =>
      val fisherFeaturizer = constructFisherFeaturizer(gmmPart, Some("sift-fisher"))
      fisherFeaturizer(pcaTransformedRDD)
    }

    val testFeatures = splitGMMs.iterator.map { gmmPart => 
      val fisherFeaturizer = constructFisherFeaturizer(gmmPart, Some("sift-fisher"))
      (grayscaler then featurizer then fisherFeaturizer).apply(testParsed)
    }

    (trainingFeatures, testFeatures)
  }

  def getLcsFeatures(
      conf: LazyImageNetSiftLcsFVConfig,
      trainParsed: RDD[Image],
      testParsed: RDD[Image])
    : (Iterator[RDD[DenseVector[Double]]], Iterator[RDD[DenseVector[Double]]]) = {

    val numImgs = trainParsed.count.toInt
    var lcsSamples: Option[RDD[DenseVector[Float]]] = None
    // Part 1a: If necessary, perform PCA on samples of the LCS features, or load a PCA matrix from
    // disk.
    val pcaTransformer = conf.lcsPcaFile match {
      case Some(fname) => new BatchPCATransformer(convert(csvread(new File(fname)), Float).t)
      case None => {
        val pcapipe = new LCSExtractor(conf.lcsStride, conf.lcsBorder, conf.lcsPatch)
        lcsSamples = Some(
          new ColumnSampler(conf.numPcaSamples, Some(numImgs)).apply(
            pcapipe(trainParsed)).cache())
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
          val lcs = new LCSExtractor(conf.lcsStride, conf.lcsBorder, conf.lcsPatch)
          new ColumnSampler(conf.numPcaSamples, Some(numImgs)).apply(lcs(trainParsed))
        }
        val vectorPCATransformer = new PCATransformer(pcaTransformer.pcaMat)
        new GaussianMixtureModelEstimator(conf.vocabSize)
          .fit(MatrixUtils.shuffleArray(
            vectorPCATransformer(samples).map(convert(_, Double)).collect()).take(1e6.toInt))
    }

    val splitGMMs = splitGMMCentroids(gmm, conf.centroidBatchSize)

    val trainingFeatures = splitGMMs.iterator.map { gmmPart =>
      val fisherFeaturizer = constructFisherFeaturizer(gmmPart, Some("lcs-fisher"))
      fisherFeaturizer(pcaTransformedRDD)
    }

    val testFeatures = splitGMMs.iterator.map { gmmPart => 
      val fisherFeaturizer = constructFisherFeaturizer(gmmPart, Some("lcs-fisher"))
      (featurizer then fisherFeaturizer).apply(testParsed)
    }

    (trainingFeatures, testFeatures)
  }

  def run(sc: SparkContext, conf: LazyImageNetSiftLcsFVConfig) {
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
    val numTestImgs = testActual.count

    val testFilenamesRDD = testParsedRDD.map(_.filename.get)

    val trainParsedImgs = (ImageExtractor).apply(parsedRDD) 
    val testParsedImgs = (ImageExtractor).apply(testParsedRDD)

    // Get SIFT + FV features
    val (trainSift, testSift) = getSiftFeatures(conf, trainParsedImgs, testParsedImgs)

    // Get LCS + FV features
    val (trainLcs, testLcs) = getLcsFeatures(conf, trainParsedImgs, testParsedImgs)

    val trainingFeatures = trainSift ++ trainLcs
    val testFeatures = testSift ++ testLcs
    // val trainingFeatures = ZipVectors(Seq(trainSift, trainLcs))
    // val testFeatures = ZipVectors(Seq(testSift, testLcs))

    // trainingFeatures.count
    // val numTestImgs = testFeatures.count

    // We have one block each of LCS and SIFT for centroidBatchSize
    val numBlocks = math.ceil(conf.vocabSize.toDouble / conf.centroidBatchSize).toInt * 2
    // NOTE(shivaram): one block only contains `centroidBatchSize` worth of SIFT/LCS features
    // (i.e. one of them not both !). So this will 2048 if centroidBatchSize is 16
    val numFeaturesPerBlock = 2 * conf.centroidBatchSize * conf.descDim // 2048 by default

    // Fit a weighted least squares model to the data.
    val model = new BlockWeightedLeastSquaresEstimator(
      numFeaturesPerBlock, 1, conf.lambda, conf.mixtureWeight).fit(
        trainingFeatures, trainingLabels, numBlocks)

    model.applyAndEvaluate(testFeatures,
      (testPredictedValues: RDD[DenseVector[Double]]) => {
        val predicted = TopKClassifier(5).apply(testPredictedValues)
        logInfo("TEST Error is " + Stats.getErrPercent(predicted, testActual, numTestImgs) + "%")
      }
    )

  }

  case class LazyImageNetSiftLcsFVConfig(
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
    centroidBatchSize: Int = 16, 
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

  def parse(args: Array[String]): LazyImageNetSiftLcsFVConfig = {
    new OptionParser[LazyImageNetSiftLcsFVConfig](appName) {
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

      opt[Int]("centroidBatchSize") action { (x,c) => c.copy(centroidBatchSize=x) }

      // Optional file to load stuff from
      opt[String]("siftPcaFile") action { (x,c) => c.copy(siftPcaFile=Some(x)) }
      opt[String]("siftGmmMeanFile") action { (x,c) => c.copy(siftGmmMeanFile=Some(x)) }
      opt[String]("siftGmmVarFile") action { (x,c) => c.copy(siftGmmVarFile=Some(x)) }
      opt[String]("siftGmmWtsFile") action { (x,c) => c.copy(siftGmmWtsFile=Some(x)) }

      opt[String]("lcsPcaFile") action { (x,c) => c.copy(lcsPcaFile=Some(x)) }
      opt[String]("lcsGmmMeanFile") action { (x,c) => c.copy(lcsGmmMeanFile=Some(x)) }
      opt[String]("lcsGmmVarFile") action { (x,c) => c.copy(lcsGmmVarFile=Some(x)) }
      opt[String]("lcsGmmWtsFile") action { (x,c) => c.copy(lcsGmmWtsFile=Some(x)) }

    }.parse(args, LazyImageNetSiftLcsFVConfig()).get
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
