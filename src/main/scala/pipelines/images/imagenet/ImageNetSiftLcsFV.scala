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

import nodes.images.external.{GMMFisherVectorEstimator, FisherVector, SIFTExtractor}
import nodes.images._
import nodes.learning._
import nodes.stats._
import nodes.util._

import utils.{Image, MatrixUtils, Stats}
import workflow.{Optimizer, Pipeline}

object ImageNetSiftLcsFV extends Serializable with Logging {
  val appName = "ImageNetSiftLcsFV"

  def computePCAandFisherBranch(
    prefix: Pipeline[Image, DenseMatrix[Float]],
    trainingData: RDD[Image],
    pcaFile: Option[String],
    gmmMeanFile: Option[String],
    gmmVarFile: Option[String],
    gmmWtsFile: Option[String],
    numColSamplesPerImage: Int,
    numPCADesc: Int,
    gmmVocabSize: Int): Pipeline[Image, DenseVector[Double]] = {

    val sampledColumns = prefix andThen
        ColumnSampler(numColSamplesPerImage) andThen
        new Cacher

    // Part 1a: If necessary, perform PCA on samples of the features, or load a PCA matrix from disk.
    // Part 2: Compute dimensionality-reduced PCA features.
    val pcaTransformer = pcaFile match {
      case Some(fname) =>
        new BatchPCATransformer(convert(csvread(new File(fname)), Float).t)
      case None =>
        val pca = sampledColumns andThen
            (ColumnPCAEstimator(numPCADesc), trainingData)

        pca.fittedTransformer
    }

    // Part 2a: If necessary, compute a GMM based on the dimensionality-reduced features, or load from disk.
    // Part 3: Compute Fisher Vectors and signed-square-root normalization.
    val fisherVectorTransformer = gmmMeanFile match {
      case Some(f) =>
        val gmm = new GaussianMixtureModel(
          csvread(new File(gmmMeanFile.get)),
          csvread(new File(gmmVarFile.get)),
          csvread(new File(gmmWtsFile.get)).toDenseVector)
        FisherVector(gmm)
      case None =>
        val fisherVector = sampledColumns andThen
            pcaTransformer andThen
            (GMMFisherVectorEstimator(gmmVocabSize), trainingData)
        fisherVector.fittedTransformer
    }

    prefix andThen
        pcaTransformer andThen
        fisherVectorTransformer andThen
        FloatToDouble andThen
        MatrixVectorizer andThen
        NormalizeRows andThen
        SignedHellingerMapper andThen
        NormalizeRows
  }

  def run(sc: SparkContext, conf: ImageNetSiftLcsFVConfig) {
    // Load the data and extract training labels.
    val parsedRDD = ImageNetLoader(
      sc,
      conf.trainLocation,
      conf.labelPath).cache().setName("trainData")

    val labelGrabber = LabelExtractor andThen
      ClassLabelIndicatorsFromIntLabels(ImageNetLoader.NUM_CLASSES) andThen
      new Cacher[DenseVector[Double]]
    val trainingLabels = labelGrabber(parsedRDD)
    val numTrainingImgs = trainingLabels.count()

    // Load test data and get actual labels
    val testParsedRDD = ImageNetLoader(
      sc,
      conf.testLocation,
      conf.labelPath).cache().setName("testData")
    val testActual = (labelGrabber andThen TopKClassifier(1)).apply(testParsedRDD)

    val trainParsedImgs = ImageExtractor.apply(parsedRDD)
    val testParsedImgs = ImageExtractor.apply(testParsedRDD)

    // Get SIFT + FV feature branch
    val siftBranchPrefix = PixelScaler andThen
        GrayScaler andThen
        new SIFTExtractor(scaleStep = conf.siftScaleStep) andThen
        BatchSignedHellingerMapper

    val siftBranch = computePCAandFisherBranch(
      siftBranchPrefix,
      trainParsedImgs,
      conf.siftPcaFile,
      conf.siftGmmMeanFile,
      conf.siftGmmVarFile,
      conf.siftGmmWtsFile,
      conf.numPcaSamples / numTrainingImgs.toInt,
      conf.descDim, conf.vocabSize)

    // Get LCS + FV feature branch
    val lcsBranchPrefix = new LCSExtractor(conf.lcsStride, conf.lcsBorder, conf.lcsPatch)
    val lcsBranch = computePCAandFisherBranch(
      lcsBranchPrefix,
      trainParsedImgs,
      conf.lcsPcaFile,
      conf.lcsGmmMeanFile,
      conf.lcsGmmVarFile,
      conf.lcsGmmWtsFile,
      conf.numPcaSamples / numTrainingImgs.toInt,
      conf.descDim, conf.vocabSize)

    // Combine the two models, and fit the weighted least squares model to the data
    val pipeline = Pipeline.gather {
      siftBranch :: lcsBranch :: Nil
    } andThen
        VectorCombiner() andThen
        new Cacher andThen
        (new BlockWeightedLeastSquaresEstimator(
          4096, 1, conf.lambda, conf.mixtureWeight, Some(2 * 2 * conf.descDim * conf.vocabSize)),
            trainParsedImgs,
            trainingLabels) andThen
        TopKClassifier(5)

    // Optimize the pipeline
    val predictor = Optimizer.execute(pipeline)
    logInfo("\n" + predictor.toDOTString)

    // Apply the model to test data and compute test error
    val numTestImgs = testActual.count()
    val testPredicted = predictor(testParsedImgs)
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
