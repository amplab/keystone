//package pipelines
//
//import org.apache.spark.SparkContext
//import nodes._
//import org.jblas.FloatMatrix
//import pipelines._
//import util.Stats
//
//
//object RandomCifar extends Serializable {
//  def main(args: Array[String]) {
//    if (args.length < 5) {
//      println("Usage: RandomCifar <master> <trainFile> <testFile> <numFilters> <lambda> [<sampleFrac>]")
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
//    val numFilters = args(3).toInt //20*500
//    val patchSize = 6
//
//    val alpha = 0.25
//
//    val poolSize = 14
//    val poolStride = 13
//
//    val lambda = args(4).toDouble
//
//    val sampleFrac: Option[Double] = if (args.length > 5) Some(args(5).toDouble) else None
//
//    val sc = new SparkContext(sparkMaster, "RandomCifar")
//
//    val dataLoader = new CifarParser andThen new CachingNode
//    val trainData = sampleFrac match {
//      case None => dataLoader(sc, trainFile)
//      case Some(f) => dataLoader(sc, trainFile).sample(false, f)
//    }
//
//    val filters = FloatMatrix.randn(numFilters, patchSize*patchSize*numChannels)
//    //val whitener = new ZCAWhitener(filters)
//    val filterArray = filters.toArray2
//
//
//    val featurizer =
//      ImageExtractor
//        .andThen(new Convolver(sc, filterArray, imageSize, imageSize, numChannels, None, true))
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
//
//    sc.stop()
//    sys.exit(0)
//  }
//}
