package pipelines.images.cifar

import scala.reflect.ClassTag

import breeze.linalg._
import breeze.numerics._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scopt.OptionParser

import evaluation.{AugmentedExamplesEvaluator, MulticlassClassifierEvaluator}
import loaders.CifarLoader

import nodes.images._
import nodes.learning.{BlockLeastSquaresEstimator, ZCAWhitener, ZCAWhitenerEstimator}
import nodes.stats.{StandardScaler, Sampler}
import nodes.util.{Cacher, ClassLabelIndicatorsFromIntLabels, MaxClassifier}

import pipelines.FunctionNode
import pipelines.Logging
import utils.{MatrixUtils, Stats, Image, ImageUtils}

object RandomPatchCifarAugmented extends Serializable with Logging {
  val appName = "RandomPatchCifarAugmented"

  class LabelAugmenter[T: ClassTag](mult: Int) extends FunctionNode[RDD[T], RDD[T]] {
    def apply(in: RDD[T]) = in.flatMap(x => Seq.fill(mult)(x))
  }

  def run(sc: SparkContext, conf: RandomCifarFeaturizerConfig) {
    // Set up some constants.
    val numClasses = 10
    val numChannels = 3
    val whitenerSize = 100000
    val augmentPatchSize = 24
    val flipChance = 0.5

    // Load up training data, and optionally sample.
    val trainData = conf.sampleFrac match {
      case Some(f) => CifarLoader(sc, conf.trainLocation).sample(false, f).cache
      case None => CifarLoader(sc, conf.trainLocation).cache
    }
    val trainImages = ImageExtractor(trainData)

    val patchExtractor = new Windower(conf.patchSteps, conf.patchSize)
      .andThen(ImageVectorizer.apply)
      .andThen(new Sampler(whitenerSize))

    val (filters, whitener): (DenseMatrix[Double], ZCAWhitener) = {
        val baseFilters = patchExtractor(trainImages)
        val baseFilterMat = Stats.normalizeRows(MatrixUtils.rowsToMatrix(baseFilters), 10.0)
        val whitener = new ZCAWhitenerEstimator(eps=conf.whiteningEpsilon).fitSingle(baseFilterMat)

        //Normalize them.
        val sampleFilters = MatrixUtils.sampleRows(baseFilterMat, conf.numFilters)
        val unnormFilters = whitener(sampleFilters)
        val unnormSq = pow(unnormFilters, 2.0)
        val twoNorms = sqrt(sum(unnormSq(*, ::)))

        ((unnormFilters(::, *) / (twoNorms + 1e-10)) * whitener.whitener.t, whitener)
    }

    val trainImagesAugmented = RandomFlipper(flipChance).apply(
      RandomPatcher(conf.numRandomPatchesAugment, augmentPatchSize, augmentPatchSize).apply(
        trainImages))

    val unscaledFeaturizer = 
      new Convolver(filters, augmentPatchSize, augmentPatchSize, numChannels, Some(whitener), true)
        .andThen(SymmetricRectifier(alpha=conf.alpha))
        .andThen(new Pooler(conf.poolStride, conf.poolSize, identity, sum(_)))
        .andThen(ImageVectorizer)
        .andThen(new Cacher[DenseVector[Double]](Some("features")))

    val featurizer = unscaledFeaturizer.andThen(new StandardScaler, trainImagesAugmented)
        .andThen(new Cacher[DenseVector[Double]])

    val labelExtractor = LabelExtractor
      .andThen(ClassLabelIndicatorsFromIntLabels(numClasses))

    val trainFeatures = featurizer(trainImagesAugmented)
    val trainLabels = labelExtractor(trainData)
    val trainLabelsAugmented = new LabelAugmenter(conf.numRandomPatchesAugment).apply(trainLabels)

    val model = new BlockLeastSquaresEstimator(4096, 1, conf.lambda.getOrElse(0.0)).fit(
      trainFeatures, trainLabels)

    val predictionPipeline = featurizer andThen model andThen new Cacher[DenseVector[Double]]

    // Do testing.
    val testData = CifarLoader(sc, conf.testLocation)
    val testImages = ImageExtractor(testData)

    val numTestAugment = 10 // 4 corners, center and flips of each of the 5
    val testImagesAugmented = CenterCornerPatcher(augmentPatchSize, augmentPatchSize, true).apply(
      testImages)

    // Create augmented image-ids by assiging a unique id to each test image and then 
    // augmenting the id
    val testImageIdsAugmented = new LabelAugmenter(numTestAugment).apply(
      testImages.zipWithUniqueId.map(x => x._2))

    val testLabelsAugmented = new LabelAugmenter(numTestAugment).apply(LabelExtractor(testData))
    val testPredictions = predictionPipeline(testImagesAugmented)

    val testEval = AugmentedExamplesEvaluator(
      testImageIdsAugmented, testPredictions, testLabelsAugmented, numClasses)
    logInfo(s"Test error is: ${testEval.totalError}")
  }

  case class RandomCifarFeaturizerConfig(
      trainLocation: String = "",
      testLocation: String = "",
      numFilters: Int = 100,
      whiteningEpsilon: Double = 0.1,
      patchSize: Int = 6,
      patchSteps: Int = 1,
      poolSize: Int = 10,
      poolStride: Int = 9,
      alpha: Double = 0.25,
      lambda: Option[Double] = None,
      sampleFrac: Option[Double] = None,
      numRandomPatchesAugment: Int = 10)

  def parse(args: Array[String]): RandomCifarFeaturizerConfig = new OptionParser[RandomCifarFeaturizerConfig](appName) {
    head(appName, "0.1")
    help("help") text("prints this usage text")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("testLocation") required() action { (x,c) => c.copy(testLocation=x) }
    opt[Int]("numFilters") action { (x,c) => c.copy(numFilters=x) }
    opt[Double]("whiteningEpsilon") required() action { (x,c) => c.copy(whiteningEpsilon=x) }
    opt[Int]("patchSize") action { (x,c) => c.copy(patchSize=x) }
    opt[Int]("patchSteps") action { (x,c) => c.copy(patchSteps=x) }
    opt[Int]("poolSize") action { (x,c) => c.copy(poolSize=x) }
    opt[Int]("numRandomPatchesAugment") action { (x,c) => c.copy(numRandomPatchesAugment=x) }
    opt[Double]("alpha") action { (x,c) => c.copy(alpha=x) }
    opt[Double]("lambda") action { (x,c) => c.copy(lambda=Some(x)) }
    opt[Double]("sampleFrac") action { (x,c) => c.copy(sampleFrac=Some(x)) }
  }.parse(args, RandomCifarFeaturizerConfig()).get

  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
   * @param args
   */
  def main(args: Array[String]) = {
    val appConfig = parse(args)

    val conf = new SparkConf().setAppName(appName)
    conf.setIfMissing("spark.master", "local[2]")
    // NOTE: ONLY APPLICABLE IF YOU CAN DONE COPY-DIR
    conf.remove("spark.jars")
    val sc = new SparkContext(conf)
    run(sc, appConfig)

    sc.stop()
  }
}
