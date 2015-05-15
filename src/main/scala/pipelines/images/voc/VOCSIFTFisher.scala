package pipelines.images.voc

import breeze.linalg._
import breeze.numerics._
import breeze.stats._
import evaluation.MeanAveragePrecisionEvaluator
import loaders.{VOCDataPath, VOCLabelPath, VOCLoader}
import nodes.images.external.{FisherVector, SIFTExtractor}
import nodes.images.{GrayScaler, Im2Single, MultiLabelExtractor, MultiLabeledImageExtractor}
import nodes.learning._
import nodes.misc.MatrixVectorizer
import nodes.stats.SignedHellingerMapper
import nodes.util.{Cacher, ClassLabelIndicatorsFromIntArrayLabels}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import utils.{Image, MatrixUtils}

object VOCSIFTFisher extends Serializable {
  val appName = "VOCSIFTFisher"

  def splitFeatures(
      in: RDD[DenseVector[Double]],
      blockSize: Int) = {
    val numFeatures = in.first.length
    val numBlocks = math.ceil(numFeatures.toDouble / blockSize).toInt
    (0 until numBlocks).map { blockNum =>
      in.map { vec =>
        // For the evilness of breeze's slice
        DenseVector(vec.slice(blockNum * blockSize, (blockNum + 1) * blockSize).toArray)
      }
    }
  }

  def run(sc: SparkContext, conf: SIFTFisherConfig) {
    //Set up some constants.
    val numClasses = VOCLoader.NUM_CLASSES
    val numPcaSamples = 1e6.toInt
    val numGmmSamples = 1e6.toInt

    //Load
    val parsedRDD = VOCLoader(
      sc,
      VOCDataPath(conf.trainLocation, "VOCdevkit/VOC2007/JPEGImages/", Some(1)),
      VOCLabelPath(conf.labelPath)).repartition(conf.numParts)

    //Part 1
    val grayscaler = MultiLabeledImageExtractor then Im2Single then GrayScaler then new Cacher[Image]
    val grayRDD = grayscaler(parsedRDD)

    def createSamples(in: RDD[DenseMatrix[Float]], numSamples: Int): RDD[DenseVector[Float]] = {
      val numImgs = in.count.toInt
      val imgsPerSample = numSamples/numImgs
      in.flatMap(mat => {
        (0 until imgsPerSample).map( x => {
          mat(::, scala.util.Random.nextInt(mat.cols)).toDenseVector
        })
      })
    }

    //If necessary, calculate the PCA
    val pcaTransformer = conf.pcaFile match {
      case Some(fname) => new BatchPCATransformer(convert(MatrixUtils.loadCSVFile(fname), Float).t)
      case None => {
        val se = SIFTExtractor(conf.scaleStep)
        val siftSamples = se(grayRDD)
        val pca = new PCAEstimator(conf.descDim).fit(createSamples(siftSamples, numPcaSamples))
        new BatchPCATransformer(pca.pcaMat)
      }
    }

    //Part 2 Compute dimensionality-reduced PCA features.
    val featurizer =  new SIFTExtractor(conf.scaleStep) then pcaTransformer then new Cacher[DenseMatrix[Float]]

    val firstCachedRDD = featurizer(grayRDD)

    val labelGrabber = ( MultiLabelExtractor
      then ClassLabelIndicatorsFromIntArrayLabels(numClasses)
      then new Cacher[DenseVector[Double]])

    val labelsRDD = labelGrabber(parsedRDD)

    //Now train a GMM based on the dimred'ed data.
    val gmm = conf.gmmMeanFile match {
      case Some(f) =>
        new GaussianMixtureModel(
          MatrixUtils.loadCSVFile(conf.gmmMeanFile.get),
          MatrixUtils.loadCSVFile(conf.gmmVarFile.get),
          MatrixUtils.loadCSVFile(conf.gmmWtsFile.get).toDenseVector)
      case None =>
        new GaussianMixtureModelEstimator(conf.vocabSize)
          .fit(createSamples(firstCachedRDD, numGmmSamples).map(convert(_, Double)))
    }

    def normalizeRows(x: DenseVector[Double]): DenseVector[Double] = {
        val norm = max(sqrt(sum(pow(x, 2.0))), 2.2e-16)
        x / norm
    }

    def doubleConverter(x: DenseMatrix[Float]): DenseMatrix[Double] = convert(x, Double)

    //Step 3
    val fisherFeaturizer =  (
      new FisherVector(gmm) then doubleConverter _
      then MatrixVectorizer
      then normalizeRows _
      then SignedHellingerMapper
      then normalizeRows _
      then new Cacher[DenseVector[Double]])

    val trainingFeatures = fisherFeaturizer(firstCachedRDD)

    val model = BlockLinearMapper.trainWithL2(splitFeatures(trainingFeatures, 4096), labelsRDD, conf.lambda, 1)

    trainingFeatures.unpersist()

    val testParsedRDD = VOCLoader(
      sc,
      VOCDataPath(conf.testLocation, "VOCdevkit/VOC2007/JPEGImages/", Some(1)),
      VOCLabelPath(conf.labelPath)).repartition(conf.numParts)

    val testCachedRDD = featurizer(grayscaler(testParsedRDD))

    println("Test Cached RDD has: " + testCachedRDD.count)
    val testFeatures = fisherFeaturizer(testCachedRDD)

    val testActuals = MultiLabelExtractor(testParsedRDD)

    val predictions = model(splitFeatures(testFeatures, 4096))

    val map = MeanAveragePrecisionEvaluator(testActuals, predictions, numClasses)
    println(s"TEST APs are: ${map.toArray.mkString(",")}")
    println(s"TEST MAP is: ${mean(map)}")
  }

  case class SIFTFisherConfig(
    trainLocation: String = "",
    testLocation: String = "",
    labelPath: String = "",
    numParts: Int = 496,
    lambda: Double = 0.5,
    descDim: Int = 80,
    vocabSize: Int = 256,
    scaleStep: Int = 0,
    pcaFile: Option[String] = None,
    gmmMeanFile: Option[String]= None,
    gmmVarFile: Option[String] = None,
    gmmWtsFile: Option[String] = None)

  def parse(args: Array[String]): SIFTFisherConfig = new OptionParser[SIFTFisherConfig](appName) {
    head(appName, "0.1")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("testLocation") required() action { (x,c) => c.copy(testLocation=x) }
    opt[String]("labelPath") required() action { (x,c) => c.copy(labelPath=x) }
    opt[Int]("numParts") action { (x,c) => c.copy(numParts=x) }
    opt[Double]("lambda") action { (x,c) => c.copy(lambda=x) }
    opt[Int]("descDim") action { (x,c) => c.copy(descDim=x) }
    opt[Int]("vocabSize") action { (x,c) => c.copy(vocabSize=x) }
    opt[Int]("scaleStep") action { (x,c) => c.copy(scaleStep=x) }
    opt[String]("pcaFile") action { (x,c) => c.copy(pcaFile=Some(x)) }
    opt[String]("gmmMeanFile") action { (x,c) => c.copy(gmmMeanFile=Some(x)) }
    opt[String]("gmmVarFile") action { (x,c) => c.copy(gmmVarFile=Some(x)) }
    opt[String]("gmmWtsFile") action { (x,c) => c.copy(gmmWtsFile=Some(x))}
  }.parse(args, SIFTFisherConfig()).get

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
