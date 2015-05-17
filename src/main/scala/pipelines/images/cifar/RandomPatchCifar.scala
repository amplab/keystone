package pipelines.images.cifar

import breeze.linalg._
import breeze.numerics._
import evaluation.MulticlassClassifierEvaluator
import loaders.CifarLoader
import nodes.images._
import nodes.learning.{LinearMapEstimator, ZCAWhitener, ZCAWhitenerEstimator}
import nodes.stats.StandardScaler
import nodes.util.{Sampler, Cacher, ClassLabelIndicatorsFromIntLabels, MaxClassifier}
import org.apache.spark.{SparkConf, SparkContext}
import pipelines.Logging
import scopt.OptionParser
import utils.{MatrixUtils, Stats}


object RandomPatchCifar extends Serializable with Logging {
  val appName = "RandomPatchCifar"

  def run(sc: SparkContext, conf: RandomCifarConfig) {
    //Set up some constants.
    val numClasses = 10
    val imageSize = 32
    val numChannels = 3
    val whitenerSize = 100000

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
        val whitener = new ZCAWhitenerEstimator().fitSingle(baseFilterMat)

        //Normalize them.
        val sampleFilters = MatrixUtils.sampleRows(baseFilterMat, conf.numFilters)
        val unnormFilters = whitener(sampleFilters)
        val unnormSq = pow(unnormFilters, 2.0)
        val twoNorms = sqrt(sum(unnormSq(*, ::)))

        ((unnormFilters(::, *) / (twoNorms + 1e-10)) * whitener.whitener.t, whitener)
    }


    val featurizer = new Convolver(filters, imageSize, imageSize, numChannels, Some(whitener), true)
        .then(SymmetricRectifier(alpha=conf.alpha))
        .then(new Pooler(conf.poolStride, conf.poolSize, identity, _.sum))
        .then(ImageVectorizer)
        .then(new Cacher[DenseVector[Double]])
        .thenEstimator(new StandardScaler).fit(trainImages)
        .then(new Cacher[DenseVector[Double]])

    val labelExtractor = LabelExtractor then ClassLabelIndicatorsFromIntLabels(numClasses) then new Cacher[DenseVector[Double]]

    val trainFeatures = featurizer(trainImages)
    val trainLabels = labelExtractor(trainData)

    val model = LinearMapEstimator(conf.lambda).fit(trainFeatures, trainLabels)

    val predictionPipeline = featurizer then model then MaxClassifier then new Cacher[Int]

    // Calculate training error.
    val trainEval = MulticlassClassifierEvaluator(predictionPipeline(trainImages), LabelExtractor(trainData), numClasses)

    // Do testing.
    val testData = CifarLoader(sc, conf.testLocation)
    val testImages = ImageExtractor(testData)
    val testLabels = labelExtractor(testData)

    val testEval = MulticlassClassifierEvaluator(predictionPipeline(testImages), LabelExtractor(testData), numClasses)

    logInfo(s"Training error is: ${trainEval.totalError}")
    logInfo(s"Test error is: ${testEval.totalError}")
  }

  case class RandomCifarConfig(
      trainLocation: String = "",
      testLocation: String = "",
      numFilters: Int = 100,
      patchSize: Int = 6,
      patchSteps: Int = 1,
      poolSize: Int = 14,
      poolStride: Int = 13,
      alpha: Double = 0.25,
      lambda: Option[Double] = None,
      sampleFrac: Option[Double] = None)

  def parse(args: Array[String]): RandomCifarConfig = new OptionParser[RandomCifarConfig](appName) {
    head(appName, "0.1")
    opt[String]("trainLocation") required() action { (x,c) => c.copy(trainLocation=x) }
    opt[String]("testLocation") required() action { (x,c) => c.copy(testLocation=x) }
    opt[Int]("numFilters") action { (x,c) => c.copy(numFilters=x) }
    opt[Int]("patchSize") action { (x,c) => c.copy(patchSize=x) }
    opt[Int]("patchSteps") action { (x,c) => c.copy(patchSteps=x) }
    opt[Int]("poolSize") action { (x,c) => c.copy(poolSize=x) }
    opt[Double]("alpha") action { (x,c) => c.copy(alpha=x) }
    opt[Double]("lambda") action { (x,c) => c.copy(lambda=Some(x)) }
    opt[Double]("sampleFrac") action { (x,c) => c.copy(sampleFrac=Some(x)) }
  }.parse(args, RandomCifarConfig()).get

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
