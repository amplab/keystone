package pipelines.images.voc

import java.io.File

import breeze.linalg._
import breeze.stats._
import evaluation.MeanAveragePrecisionEvaluator
import loaders.{VOCDataPath, VOCLabelPath, VOCLoader}
import nodes.images.external.SIFTExtractor
import nodes.images.{GrayScaler, MultiLabelExtractor, MultiLabeledImageExtractor, PixelScaler}
import nodes.learning._
import nodes.stats.{BatchSignedHellingerMapper, ColumnSampler, NormalizeRows, SignedHellingerMapper}
import nodes.util.{Cacher, ClassLabelIndicatorsFromIntArrayLabels, FloatToDouble, MatrixVectorizer}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import utils.{MatrixUtils, Image}

object BensVOCSIFTInteractionTerms extends Serializable {
  val appName = "VOCSIFTInteractionTerms"

  def run(sc: SparkContext, conf: SIFTInteractionTermsConfig) {

    // Load the data and extract training labels.
    val parsedRDD = VOCLoader(
      sc,
      VOCDataPath(conf.trainLocation, "VOCdevkit/VOC2007/JPEGImages/", Some(1)),
      VOCLabelPath(conf.labelPath)).repartition(conf.numParts)

    val labelGrabber = MultiLabelExtractor then
      ClassLabelIndicatorsFromIntArrayLabels(VOCLoader.NUM_CLASSES) then
      new Cacher[DenseVector[Double]]

    val trainingLabels = labelGrabber(parsedRDD)

    // Part 1: Scale and convert images to grayscale, and extract the sifts
    val grayscalerAndSift = MultiLabeledImageExtractor then
        PixelScaler then
        GrayScaler then
        new Cacher[Image] then
        new SIFTExtractor(scaleStep = conf.scaleStep) then
        BatchSignedHellingerMapper then FloatToDouble thenFunction { x =>
          val rows = MatrixUtils.matrixToRowArray(x.t)
          DenseVector.horzcat(rows.filter(x => norm(x, 2) > 1e-3):_*)
        }

    val siftRDD = grayscalerAndSift(parsedRDD)

    // Part 2: Train the ZCA Whitener & template filters & gaussian kitchen sinks in the InteractionTermsEstimator.
    val featurizer = BensInteractionTermsEstimator(conf.numTemplateFilters, conf.numGaussianFilters, conf.numSamples).fit(siftRDD) then
        MatrixVectorizer then
        NormalizeRows then
        SignedHellingerMapper then
        NormalizeRows then
        new Cacher[DenseVector[Double]]

    val trainingFeatures = featurizer(siftRDD)

    // Part 4: Fit a linear model to the data.
    val model = new BlockLeastSquaresEstimator(4096, 1, conf.lambda).fit(
      trainingFeatures, trainingLabels, Some(conf.numGaussianFilters * conf.numTemplateFilters))

    siftRDD.unpersist()
    trainingFeatures.unpersist()

    // Now featurize and apply the model to test data.
    val testParsedRDD = VOCLoader(
      sc,
      VOCDataPath(conf.testLocation, "VOCdevkit/VOC2007/JPEGImages/", Some(1)),
      VOCLabelPath(conf.labelPath)).repartition(conf.numParts)

    val testFeatures = featurizer(grayscalerAndSift(testParsedRDD))

    println("Test Cached RDD has: " + testFeatures.count)
    val testActuals = MultiLabelExtractor(testParsedRDD)

    val predictions = model(testFeatures)

    val map = MeanAveragePrecisionEvaluator(testActuals, predictions, VOCLoader.NUM_CLASSES)
    println(s"TEST APs are: ${map.toArray.mkString(",")}")
    println(s"TEST MAP is: ${mean(map)}")
  }

  case class SIFTInteractionTermsConfig(
    trainLocation: String = "",
    testLocation: String = "",
    labelPath: String = "",
    numParts: Int = 496,
    lambda: Double = 0.5,
    numTemplateFilters: Int = 256,
    numGaussianFilters: Int = 160,
    scaleStep: Int = 0,
    numSamples: Int = 1e6.toInt)

  def parse(args: Array[String]): SIFTInteractionTermsConfig = new OptionParser[SIFTInteractionTermsConfig](appName) {
    head(appName, "0.1")
    help("help") text("prints this usage text")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("testLocation") required() action { (x,c) => c.copy(testLocation=x) }
    opt[String]("labelPath") required() action { (x,c) => c.copy(labelPath=x) }
    opt[Int]("numParts") action { (x,c) => c.copy(numParts=x) }
    opt[Double]("lambda") action { (x,c) => c.copy(lambda=x) }
    opt[Int]("numTemplateFilters") action { (x,c) => c.copy(numTemplateFilters=x) }
    opt[Int]("numGaussianFilters") action { (x,c) => c.copy(numGaussianFilters=x) }
    opt[Int]("scaleStep") action { (x,c) => c.copy(scaleStep=x) }
    opt[Int]("numSamples") action { (x,c) => c.copy(numSamples=x) }
  }.parse(args, SIFTInteractionTermsConfig()).get

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
