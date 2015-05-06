//package pipelines
//
//import org.apache.spark.SparkContext
//import nodes._
//import org.jblas.{MatrixFunctions, FloatMatrix}
//import pipelines._
//import util.{PipelinePersistence, Stats}
//
//import scala.util.Random
//
//
//object RandomPatchCifar extends Serializable {
//  def main(args: Array[String]) {
//    if (args.length < 5) {
//      println("Usage: RandomCifar <master> <trainFile> <testFile> <numFilters> <lambda>")
//      System.exit(0)
//    }
//
//    val sparkMaster = args(0)
//    val trainFile = args(1)
//    val testFile = args(2)
//
//    //Set up some constants.
//    val numClasses = 10
//    val imageSize = 32
//    val numChannels = 3
//
//    val numFilters = args(3).toInt
//    val patchSize = 6
//    val patchSteps = 3
//    val whitenerSize = 100000
//
//    val alpha = 0.25
//
//    val poolSize = 14
//    val poolStride = 13
//
//    val lambda = args(4).toDouble
//
//    val sc = new SparkContext(sparkMaster, "RandomPatchCifar")
//
//    val dataLoader = new CifarParser// andThen new CachingNode
//    val trainData = dataLoader(sc, trainFile).sample(false, 0.2).cache()
//
//    val patchExtractor = ImageExtractor
//      .andThen(new Windower(patchSteps,patchSize))
//      .andThen(new ImageVectorizer)
//      .andThen(new Sampler(whitenerSize))
//
//    val (filters, whitener) = {
//        val baseFilters = patchExtractor(trainData)
//        val baseFilterMat = Stats.normalizeRows(new MatrixType(baseFilters), 10.0)
//        val whitener = new ZCAWhitener(baseFilterMat)
//
//        //Normalize them.
//        val sampleFilters = new MatrixType(Random.shuffle(baseFilterMat.toArray2.toList).toArray.slice(0, numFilters))
//        val unnormFilters = whitener(sampleFilters)
//        val twoNorms = MatrixFunctions.pow(MatrixFunctions.pow(unnormFilters, 2.0).rowSums, 0.5)
//
//        (((unnormFilters divColumnVector (twoNorms.addi(1e-10))) mmul (whitener.whitener.transpose)).toArray2, whitener)
//    }
//
//
//    val featurizer =
//      ImageExtractor
//        .andThen(new Convolver(sc, filters, imageSize, imageSize, numChannels, Some(whitener), true))
//        .andThen(SymmetricRectifier(alpha=alpha))
//        .andThen(new Pooler(poolStride, poolSize, identity, _.sum))
//        .andThen(new ImageVectorizer)
//        .andThen(new CachingNode)
//        .andThen(new FeatureNormalize)
//        .andThen(new InterceptAdder)
//        .andThen(new CachingNode)
//
//    val labelExtractor = LabelExtractor andThen ClassLabelIndicatorsFromIntLabels(numClasses) andThen new CachingNode
//
//    val trainFeatures = featurizer(trainData)
//    val trainLabels = labelExtractor(trainData)
//
//
//    val model = LinearMapper.trainWithL2(trainFeatures, trainLabels, lambda)
//
//    val predictionPipeline = featurizer andThen model andThen new CachingNode
//
//    //Calculate training error.
//    val trainError = Stats.classificationError(predictionPipeline(trainData), trainLabels)
//
//    //Do testing.
//    val testData = dataLoader(sc, testFile)
//    val testLabels = labelExtractor(testData)
//
//    val testError = Stats.classificationError(predictionPipeline(testData), testLabels)
//
//    println(s"Training error is: $trainError, Test error is: $testError")
//
//    PipelinePersistence.savePipeline(predictionPipeline, "saved_pipelines/randomPatchCifar.pipe")
//
//
//    sc.stop()
//    sys.exit(0)
//  }
//}
