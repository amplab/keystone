package pipelines.speech

import breeze.linalg.DenseMatrix
import breeze.stats.distributions.{RandBasis, ThreadLocalRandomGenerator}
import evaluation.MulticlassClassifierEvaluator
import loaders.CsvDataLoader
import nodes.learning.BlockLinearMapper
import nodes.misc.{ClassLabelIndicators, CosineRandomFeatures, MaxClassifier, StandardScaler}
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import pipelines._
import utils.MatrixUtils

import scala.collection.mutable


object TimitPipeline extends Logging {

  def main(args: Array[String]) {
    if (args.length < 11) {
      println("Usage: TimitRandomCosineMultiPassPipeline <master> <trainingFeaturesFile> " +
          " <trainingLabelsFile> <testFeaturesFile> <testLabelsFile> <numParts> <numCosines> " +
          " <gamma> <cauchy|gaussian> <lambda> <numPasses> [modelFileName] [checkPointDir]")
      System.exit(0)
    }

    // Get the command-line args & construct the spark context
    val sparkMaster = args(0)
    val trainingFile = args(1)
    val trainingLabels = args(2)
    val testFile = args(3)
    val testActual = args(4)
    val numParts = args(5).toInt
    val numCosines = args(6).toInt
    val gamma = args(7).toDouble
    val rfType = args(8) match {
      case "cauchy" => args(8)
      case "gaussian" => args(8)
      case _ => {
        println("Invalid random feature type. Using gaussian")
        "gaussian"
      }
    }

    val lambda = args(9).toDouble
    val numEpochs = args(10).toInt
    val modelFileName = if (args.length > 11) Some(args(11)) else None
    val checkpointDir = if (args.length > 12) Some(args(12)) else None

    val conf = new SparkConf().setMaster(sparkMaster)
        .setAppName("TimitRandomCosineMultiPassPipeline")
        .setJars(SparkContext.jarOfObject(this).toSeq)
    val sc = new SparkContext(conf)
    checkpointDir.foreach(_ => sc.setCheckpointDir(_))

    Thread.sleep(5000)

    // Set the constants
    val seed = 123L
    val random = new java.util.Random(seed)
    val randomSignSource: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(random.nextLong())))

    val numCosineFeatures = 4096
    val numCosineBatches = numCosines
    val colsPerBatch = numCosineFeatures + 1

    // Build the pipeline
    val trainData = CsvDataLoader(sc, trainingFile, numParts).cache().setName("trainRaw")
    trainData.count()

    val batchFeaturizer = (0 until numCosineBatches).map { batch =>
      if (rfType == "cauchy") {
        CosineRandomFeatures.createCauchyCosineRF(
          TimitUtils.timitDimension,
          numCosineFeatures,
          gamma,
          randomSignSource)
      } else {
        CosineRandomFeatures.createGaussianCosineRF(
          TimitUtils.timitDimension,
          numCosineFeatures,
          gamma,
          randomSignSource)
      }.thenEstimator(new StandardScaler()).fit(trainData)
    }

    val trainingBatches = batchFeaturizer.map { x =>
      x.apply(trainData)
    }

    val labels = TimitUtils.createTrainLabelsRDD(
      TimitUtils.parseSparseLabels(trainingLabels),
      trainData,
      TimitUtils.numClasses).cache().setName("trainLabels")

    val testData = CsvDataLoader(sc, testFile, numParts).cache().setName("testRaw")
    val numTest = testData.count()

    val testBatches = batchFeaturizer.map { case x =>
      val rdd = x.apply(testData)
      rdd.cache().setName("testFeatures")
    }

    val actual = TimitUtils.createActualLabelsRDD(
      TimitUtils.parseSparseLabels(testActual), testData).cache().setName("actual")

    // Train the model
    val blockLinearMapper = BlockLinearMapper.trainWithL2(trainingBatches, labels, lambda, numEpochs)

    // Calculate test error
    blockLinearMapper.applyAndEvaluate(testBatches, testPredictedValues => {
      val predicted = MaxClassifier(testPredictedValues)
      val evaluator = MulticlassClassifierEvaluator(predicted, actual, TimitUtils.numClasses)
      println("TEST Error is " + (100d - 100d * evaluator.microAccuracy) + "%")
    })

    TimitUtils.saveTimitModelAndSVD(blockLinearMapper.xs, modelFileName)

    System.exit(0)
  }
}

object TimitUtils {

  val timitDimension = 440
  val numClasses = 147

  // Assumes lines are formatted as
  // row col value
  def parseSparseLabels(fileName: String) = {
    // Mapping from row number to label
    val ret = new mutable.HashMap[Long, Int]

    val lines = scala.io.Source.fromFile(fileName).getLines()
    lines.foreach { line =>
      val parts = line.split(" ")
      ret(parts(0).toLong - 1) = parts(1).toInt
    }
    ret
  }

  def createActualLabelsRDD(
      labelsMap: mutable.HashMap[Long, Int],
      featuresRDD: RDD[_]) = {
    val labelsMapBC = featuresRDD.context.broadcast(labelsMap)
    val labelsRDD = featuresRDD.zipWithIndex().map { case (item, row) =>
      labelsMapBC.value(row) - 1
    }
    labelsRDD
  }

  def createTrainLabelsRDD(
      labelsMap: mutable.HashMap[Long, Int],
      featuresRDD: RDD[_],
      numClasses: Int) = {
    val labelsMapBC = featuresRDD.context.broadcast(labelsMap)
    val labelsRDD = featuresRDD.zipWithIndex().map { case (item, row) =>
      labelsMapBC.value(row)
    }
    val indicatorExtractor = ClassLabelIndicators(numClasses)
    indicatorExtractor.apply(labelsRDD)
  }

  def saveTimitModelAndSVD(
      models: Seq[DenseMatrix[Double]],
      modelFileName: Option[String]) = {

    modelFileName.foreach { mf =>
      // Save out the model for the first lambda to a file
      // Convert it to doubles before saving
      val fullModel = models.reduceLeft {
        (a: DenseMatrix[Double], b: DenseMatrix[Double]) => DenseMatrix.vertcat(a, b)
      }

      println("Dimensions of full model: " + fullModel.rows + "x" + fullModel.cols)

      MatrixUtils.writeCSVFile(mf, fullModel)

      // Compute Singular values of model
      val singularValues = breeze.linalg.svd(fullModel).S
      // print singular values
      println("Singular values of model\n " + singularValues.data.mkString("\n"))
    }
  }
}

