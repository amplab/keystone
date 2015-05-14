package pipelines.images.voc

import breeze.linalg._
import breeze.numerics._
import breeze.stats._
import edu.berkeley.cs.amplab.mlmatrix._
import evaluation.MeanAveragePrecisionEvaluator
import loaders.{VOCDataPath, VOCLabelPath, VOCLoader}
import nodes.images.external.{FisherVector, SIFTExtractor}
import nodes.images.{GrayScaler, MultiLabelExtractor, MultiLabeledImageExtractor}
import nodes.learning._
import nodes.misc.{MatrixVectorizer, StandardScaler}
import nodes.stats.SignedHellingerMapper
import nodes.util.{Cacher, ClassLabelIndicatorsFromIntArrayLabels}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import pipelines.Transformer
import utils.{Image, ImageUtils, MatrixUtils}

object Im2Single extends Transformer[Image,Image] {
  def apply(im: Image): Image = {
    ImageUtils.mapPixels(im, _/255.0)
  }
}

object FVVOC2007 extends Serializable {

  def splitMatrixParts(A: RowPartitionedMatrix, colBlockSize: Int): Iterator[RowPartitionedMatrix] = {
    // Step 1: Split the RowPartitionedMatrix into a list of RowPartitionedMatrix such that
    // each of them has colBlockSize columns.
    val numColsA = A.numCols()

    // If the last block will be smaller than colBlockSize/2 just merge it with previous block
    val numColBlocks = if (numColsA % colBlockSize < colBlockSize/2) {
      (numColsA / colBlockSize).toInt
    } else {
      (numColsA / colBlockSize).toInt + 1
    }

    val aParts = (0 until numColBlocks).map { colBlock =>
      // get columns from colBlock to colBlock + colBlockSize
      if (colBlock != numColBlocks - 1) {
        A(::, colBlock * colBlockSize until colBlock * colBlockSize + colBlockSize)
      } else {
        A(::, colBlock * colBlockSize until numColsA.toInt)
      }
    }
    println("Created aParts " + aParts.length + " " + numColBlocks)
    aParts.iterator
  }

  def main(args: Array[String]) {
    if (args.length < 8) {
      println("Usage: VOCSIFTFisher <master> <trainFile> <testFile> <labelPath> <parts> <lambda> <descdim> <vocabSize> [siftfile gmmmufile gmmvarfile gmmpriorfile]")
      System.exit(0)
    }

    val sparkMaster = args(0)
    val trainingDirName = args(1)
    val testingDirName = args(2)
    val labelPath = args(3)
    val numParts = args(4).toInt
    val lambda = args(5).toDouble
    val descDim = args(6).toInt
    val vocabSize = args(7).toInt

    //Set up some constants.
    val numClasses = VOCLoader.NUM_CLASSES
    val imageSize = 480 //This is the size of the row dimension. Comes from standardizeImage.m
    val numChannels = 3
    val numPcaSamples = 1e6.toInt

    val siftSize  = 3

    val conf = new SparkConf().setMaster(sparkMaster)
      .setAppName("VOCSIFTFisher")
      .setJars(SparkContext.jarOfObject(this).toSeq)
    val sc = new SparkContext(conf)
    Thread.sleep(5000)

    //Load
    val parsedRDD = VOCLoader(sc, VOCDataPath(trainingDirName, "VOCdevkit/VOC2007/JPEGImages/", Some(1)), VOCLabelPath(labelPath)).repartition(numParts)
    val filenamesRDD = parsedRDD.map(_.filename.get)

    //Part 1
    val grayscaler = MultiLabeledImageExtractor then Im2Single then GrayScaler
    val grayRDD = grayscaler(parsedRDD).cache()
    //makeImageCsv(filenamesRDD, grayRDD).saveAsTextFile("/testing/grayfeatures")

    def createSamples(in: RDD[DenseMatrix[Float]], numSamples: Int = 1000000): RDD[DenseVector[Float]] = {
      val numImgs = in.count.toInt
      val imgsPerSample = numSamples/numImgs
      in.flatMap(mat => {
        (0 until imgsPerSample).map( x => {
          mat(::, scala.util.Random.nextInt(mat.cols)).toDenseVector
        })
      })
    }


    //If necessary, calculate the PCA
    val pcaTransformer = if (args.length > 8) {
      new BatchPCATransformer(convert(MatrixUtils.loadCSVFile(args(8)), Float).t)
    } else {
      val se = SIFTExtractor()
      val siftSamples = se(grayRDD)
      val pca = new PCAEstimator(descDim).fit(createSamples(siftSamples, numPcaSamples))
      new BatchPCATransformer(pca.pcaMat)
    }

    //Part 2 Compute dimensionality-reduced PCA features.
    val featurizer =  new SIFTExtractor then pcaTransformer

    val firstCachedRDD = featurizer(grayRDD).cache()


    val labelGrabber = ( MultiLabelExtractor
      then ClassLabelIndicatorsFromIntArrayLabels(numClasses)
      then new Cacher[DenseVector[Double]])

    val labelsRDD = labelGrabber(parsedRDD)

    val labels = RowPartitionedMatrix.fromArray(labelsRDD.map(_.toArray))

    //Now train a GMM based on the dimred'ed data.
    val gmm = if (args.length > 9) {
      new GaussianMixtureModel(
        MatrixUtils.loadCSVFile(args(9)),
        MatrixUtils.loadCSVFile(args(10)),
        MatrixUtils.loadCSVFile(args(11)).toDenseVector)
    } else {
      new GaussianMixtureModelEstimator(vocabSize).fit(createSamples(firstCachedRDD).map(convert(_, Double)))
    }

    def normalizeRows(x: DenseVector[Double]): DenseVector[Double] = {
        val norm = max(sum(sqrt(x)), 2.2e-16)
        x / norm
    }

    def doubleConverter(x: DenseMatrix[Float]): DenseMatrix[Double] = convert(x, Double)

    //Step 3
    val fisherFeaturizer = new FisherVector(gmm) then doubleConverter _
    val fisherFeatures = fisherFeaturizer(firstCachedRDD).cache()

    //Step 4
    val normalizingFeaturizers =  (MatrixVectorizer
      then normalizeRows _
      then SignedHellingerMapper
      then normalizeRows _
      then new Cacher[DenseVector[Double]])
      

    val trainingFeatures = RowPartitionedMatrix.fromArray(normalizingFeaturizers(fisherFeatures).map(_.toArray))
    
    println("Computing model.")

    val model = new BlockCoordinateDescent().solveOnePassL2(
      splitMatrixParts(trainingFeatures, 4096), labels, Array(lambda), new NormalEquations()).map(x => x(0))
    println("Model Computed.")

    trainingFeatures.rdd.unpersist()

    val testParsedRDD = VOCLoader(sc, VOCDataPath(testingDirName, "VOCdevkit/VOC2007/JPEGImages/", Some(1)), VOCLabelPath(labelPath)).repartition(numParts)
    val testCachedRDD = featurizer(grayscaler(testParsedRDD))

    println("Test Cached RDD has: " + testCachedRDD.count)
    val testFeatures = normalizingFeaturizers(fisherFeaturizer(testCachedRDD))

    val testLabels = labelGrabber(testParsedRDD)

    val testActuals = testLabels.map { x =>
      x.findAll(_ > 0.0).toArray
    }

    val testFeaturesMat = RowPartitionedMatrix.fromArray(testFeatures.map(_.toArray))

    val fullModel = model.reduceLeftOption((a,b) => DenseMatrix.vertcat(a, b)).getOrElse(new DenseMatrix[Double](0, 0))

    val predictions = new BlockLinearMapper(Seq(fullModel)).apply(Seq(testFeatures))

    val map = MeanAveragePrecisionEvaluator(testActuals, predictions, numClasses)
    println(s"TEST APs are: ${map.toArray.mkString(",")}")
    println(s"TEST MAP is: ${mean(map)}")

    sc.stop()
    sys.exit(0)
  }
}
