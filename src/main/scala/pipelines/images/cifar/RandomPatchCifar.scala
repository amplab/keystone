package pipelines

import breeze.linalg.DenseVector
import nodes.images._
import nodes.learning.LinearMapEstimator
import nodes.math.InterceptAdder
import nodes.util.nodes.Sampler
import nodes.util.{Cacher, ClassLabelIndicatorsFromIntLabels}
import org.apache.spark.{SparkConf, SparkContext}
import nodes._
import org.apache.spark.rdd.RDD
import org.jblas.{MatrixFunctions, FloatMatrix}
import pipelines._
import scopt.OptionParser
import utils.{MatrixUtils, Stats}

import scala.util.Random


object RandomPatchCifar extends Serializable {
  val appName = "RandomPatchCifar"

  def run(sc: SparkContext, conf: RandomCifarConfig) {
    //Set up some constants.
    val numClasses = 10
    val imageSize = 32
    val numChannels = 3
    val whitenerSize = 100000


    val dataLoader = CifarLoader
    val trainData = dataLoader(sc, conf.trainLocation).sample(false, 0.2).cache()
    val trainImages = ImageExtractor(trainData)

    val x = new Windower(conf.patchSteps, conf.patchSize)

    val windower = new Windower(conf.patchSteps, conf.patchSize)
      .andThen(ImageVectorizer)
      .andThen(new Sampler(whitenerSize))

    val (filters, whitener) = {
        val baseFilters = patchExtractor(windower(trainImages).flatMap(identity))
        val baseFilterMat = Stats.normalizeRows(MatrixUtils.rowsToMatrix(baseFilters), 10.0)
        val whitener = new ZCAWhitener(baseFilterMat)

        //Normalize them.
        val sampleFilters = new MatrixType(Random.shuffle(baseFilterMat.toArray2.toList).toArray.slice(0, numFilters))
        val unnormFilters = whitener(sampleFilters)
        val twoNorms = MatrixFunctions.pow(MatrixFunctions.pow(unnormFilters, 2.0).rowSums, 0.5)

        (((unnormFilters divColumnVector (twoNorms.addi(1e-10))) mmul (whitener.whitener.transpose)).toArray2, whitener)
    }


    val featurizer = new Convolver(sc, filters, imageSize, imageSize, numChannels, Some(whitener), true)
        .then(SymmetricRectifier(alpha=conf.alpha))
        .then(new Pooler(conf.poolStride, conf.poolSize, identity, _.sum))
        .then(ImageVectorizer)
        .then(new Cacher[DenseVector[Double]])
        .thenEstimator(new FeatureNormalize).fit(trainData)
        .then(InterceptAdder)
        .then(new Cacher[DenseVector[Double]])

    val labelExtractor = LabelExtractor then ClassLabelIndicatorsFromIntLabels(numClasses) then new Cacher[DenseVector[Double]]

    val trainFeatures = featurizer(trainImages)
    val trainLabels = labelExtractor(trainData)


    val model = LinearMapEstimator(conf.lambda).fit(trainFeatures, trainLabels)

    val predictionPipeline = featurizer then model then new CachingNode[DenseVector[Double]]

    //Calculate training error.
    val trainError = Stats.classificationError(predictionPipeline(trainImages), trainLabels)

    //Do testing.
    val testData = dataLoader(sc, conf.testLocation)
    val testLabels = labelExtractor(testData)

    val testError = Stats.classificationError(predictionPipeline(testImages), testLabels)

    println(s"Training error is: $trainError, Test error is: $testError")
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
