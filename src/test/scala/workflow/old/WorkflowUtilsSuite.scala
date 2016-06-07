package workflow

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines.{PipelineContext, Logging}

class WorkflowUtilsSuite extends FunSuite with PipelineContext with Logging {
  val doubleTransformer = new Transformer[Int, Int] {
    override def apply(in: Int): Int = in * 2
    override val label = "x * 2"
  }

  val quadrupleTransformer = new Transformer[Int, Int] {
    override def apply(in: Int): Int = in * 4
    override val label = "x * 4"
  }

  val minusThreeTransformer = new Transformer[Int, Int] {
    override def apply(in: Int): Int = in - 3
    override val label = "x - 3"
  }

  val plusDataEstimator = new Estimator[Int, Int] {
    protected def fit(data: RDD[Int]): Transformer[Int, Int] = {
      val first = data.first()
      Transformer(x => x + first)
    }
    override val label = "x + data.first()"
  }

  val identityEstimator = new Estimator[Int, Int] {
    protected def fit(data: RDD[Int]): Transformer[Int, Int] = {
      Transformer(x => x)
    }
    override val label = "x"
  }

  val plusDataPlusLabelEstimator = new LabelEstimator[Int, Int, String] {
    protected def fit(data: RDD[Int], labels: RDD[String]): Transformer[Int, Int] = {
      val first = data.first() + labels.first().toInt
      Transformer(x => x + first)
    }
    override val label = "x + data.first() + label.first()"
  }

  test("Pipeline to Instructions to Pipeline") {
    sc = new SparkContext("local", "test")

    val trainData = sc.parallelize(Seq(32, 94, 12))

    val firstPipeline = doubleTransformer andThen minusThreeTransformer

    val secondPipeline = doubleTransformer andThen (plusDataEstimator, trainData)

    val labelData = sc.parallelize(Seq("10", "7", "14"))
    val thirdPipeline = quadrupleTransformer andThen
      (identityEstimator, trainData) andThen
      (plusDataPlusLabelEstimator, trainData, labelData)

    val initialPipeline = new Optimizer {
      protected val batches: Seq[Batch] = Batch("DAG Optimization", FixedPoint(100), EquivalentNodeMergeRule) :: Nil
    }.execute(Pipeline.gather {
      firstPipeline :: secondPipeline :: thirdPipeline :: Nil
    })

    val pipelineInstructions = WorkflowUtils.pipelineToInstructions(initialPipeline)

    // This assert will have to be updated if we start merging EstimatorNodes and TransformerNodes,
    // or if we change the order in which we walk the DAG
    assert(pipelineInstructions === Seq(
      doubleTransformer, // 0
      TransformerApplyNode(0, Seq(Pipeline.SOURCE)), // 1
      minusThreeTransformer, // 2
      TransformerApplyNode(2, Seq(1)), // 3
      SourceNode(trainData), // 4
      doubleTransformer, // 5
      TransformerApplyNode(5, Seq(4)), // 6
      plusDataEstimator, // 7
      EstimatorFitNode(7, Seq(6)), // 8
      TransformerApplyNode(8, Seq(1)), // 9
      quadrupleTransformer, // 10
      TransformerApplyNode(10, Seq(4)), // 11
      identityEstimator, // 12
      EstimatorFitNode(12, Seq(11)), // 13
      TransformerApplyNode(13, Seq(11)), // 14
      SourceNode(labelData), // 15
      plusDataPlusLabelEstimator, // 16
      EstimatorFitNode(16, Seq(14, 15)), // 17
      quadrupleTransformer, // 18
      TransformerApplyNode(18, Seq(Pipeline.SOURCE)), // 19
      TransformerApplyNode(13, Seq(19)), // 20
      TransformerApplyNode(17, Seq(20)), // 21
      GatherTransformer(), // 22
      TransformerApplyNode(22, Seq(3, 9, 21)) // 23
    ))
    val pipeline = WorkflowUtils.instructionsToPipeline[Int, Seq[Int]](pipelineInstructions)

    val single = 7
    assert {
      pipeline(single) === Seq(
        firstPipeline.apply(single),
        secondPipeline.apply(single),
        thirdPipeline.apply(single))
    }

    val data = Seq(13, 2, 83)
    val correctOut = data.map { x => Seq(
      firstPipeline.apply(x),
      secondPipeline.apply(x),
      thirdPipeline.apply(x))
    }
    assert(pipeline(sc.parallelize(data)).collect().toSeq === correctOut)
  }

  test("Instruction removal test") {
    sc = new SparkContext("local", "test")

    val trainData = sc.parallelize(Seq(32, 94, 12))
    val labelData = sc.parallelize(Seq("10", "7", "14"))

    val pipelineInstructions = Seq(
      doubleTransformer, // 0
      TransformerApplyNode(0, Seq(Pipeline.SOURCE)), // 1
      minusThreeTransformer, // 2
      TransformerApplyNode(2, Seq(1)), // 3
      SourceNode(trainData), // 4
      doubleTransformer, // 5
      TransformerApplyNode(5, Seq(4)), // 6
      plusDataEstimator, // 7
      doubleTransformer, // 8 - to remove
      EstimatorFitNode(7, Seq(6)), // 9
      TransformerApplyNode(9, Seq(1)), // 10
      quadrupleTransformer, // 11
      TransformerApplyNode(11, Seq(4)), // 12
      identityEstimator, // 13
      EstimatorFitNode(13, Seq(12)), // 14
      TransformerApplyNode(14, Seq(12)), // 15
      identityEstimator, // 16 - to remove
      SourceNode(labelData), // 17
      plusDataPlusLabelEstimator, // 18
      EstimatorFitNode(18, Seq(15, 17)), // 19
      quadrupleTransformer, // 20
      TransformerApplyNode(20, Seq(Pipeline.SOURCE)), // 21
      TransformerApplyNode(14, Seq(21)), // 22
      TransformerApplyNode(19, Seq(22)), // 23
      GatherTransformer(), // 24
      TransformerApplyNode(24, Seq(3, 10, 23)) // 25
    )

    // Make sure dependency breaking throws an exception
    intercept[RuntimeException] {
      WorkflowUtils.removeInstructions(Set(7, 12), pipelineInstructions)
    }

    val instructionsRemoved = WorkflowUtils.removeInstructions(Set(8, 16), pipelineInstructions)._1

    assert(instructionsRemoved === Seq(
      doubleTransformer, // 0
      TransformerApplyNode(0, Seq(Pipeline.SOURCE)), // 1
      minusThreeTransformer, // 2
      TransformerApplyNode(2, Seq(1)), // 3
      SourceNode(trainData), // 4
      doubleTransformer, // 5
      TransformerApplyNode(5, Seq(4)), // 6
      plusDataEstimator, // 7
      EstimatorFitNode(7, Seq(6)), // 8
      TransformerApplyNode(8, Seq(1)), // 9
      quadrupleTransformer, // 10
      TransformerApplyNode(10, Seq(4)), // 11
      identityEstimator, // 12
      EstimatorFitNode(12, Seq(11)), // 13
      TransformerApplyNode(13, Seq(11)), // 14
      SourceNode(labelData), // 15
      plusDataPlusLabelEstimator, // 16
      EstimatorFitNode(16, Seq(14, 15)), // 17
      quadrupleTransformer, // 18
      TransformerApplyNode(18, Seq(Pipeline.SOURCE)), // 19
      TransformerApplyNode(13, Seq(19)), // 20
      TransformerApplyNode(17, Seq(20)), // 21
      GatherTransformer(), // 22
      TransformerApplyNode(22, Seq(3, 9, 21)) // 23
    ))
  }

  test("Instruction splicing") {
    sc = new SparkContext("local", "test")

    val trainData = sc.parallelize(Seq(32, 94, 12))
    val labelData = sc.parallelize(Seq("10", "7", "14"))

    val pipelineInstructions = Seq(
      doubleTransformer, // 0
      TransformerApplyNode(0, Seq(Pipeline.SOURCE)), // 1
      minusThreeTransformer, // 2
      TransformerApplyNode(2, Seq(1)), // 3
      SourceNode(trainData), // 4
      doubleTransformer, // 5
      TransformerApplyNode(5, Seq(4)), // 6
      plusDataEstimator, // 7
      EstimatorFitNode(7, Seq(6)), // 8
      TransformerApplyNode(8, Seq(1)), // 9
      quadrupleTransformer, // 10 - splice with another minusThreeTransformer
      TransformerApplyNode(10, Seq(4)), // 11 - splice with an apply for the new doubleTransformer
      identityEstimator, // 12
      EstimatorFitNode(12, Seq(11)), // 13
      TransformerApplyNode(13, Seq(11)), // 14
      SourceNode(labelData), // 15
      plusDataPlusLabelEstimator, // 16
      EstimatorFitNode(16, Seq(14, 15)), // 17
      quadrupleTransformer, // 18
      TransformerApplyNode(18, Seq(Pipeline.SOURCE)), // 19
      TransformerApplyNode(13, Seq(19)), // 20
      TransformerApplyNode(17, Seq(20)), // 21
      GatherTransformer(), // 22
      TransformerApplyNode(22, Seq(3, 9, 21)) // 23
    )

    val splice = Seq(
      minusThreeTransformer,
      TransformerApplyNode(0, Seq(Pipeline.SOURCE))
    )

    val splicedInstructions = WorkflowUtils.spliceInstructions(
      splice,
      pipelineInstructions,
      Map(Pipeline.SOURCE -> 4),
      11)._1

    assert(splicedInstructions === Seq(
      doubleTransformer, // 0
      TransformerApplyNode(0, Seq(Pipeline.SOURCE)), // 1
      minusThreeTransformer, // 2
      TransformerApplyNode(2, Seq(1)), // 3
      SourceNode(trainData), // 4
      minusThreeTransformer, // 5 - the instructions spliced in
      TransformerApplyNode(5, Seq(4)), // 6 - the instructions spliced in
      doubleTransformer, // 7
      TransformerApplyNode(7, Seq(4)), // 8
      plusDataEstimator, // 9
      EstimatorFitNode(9, Seq(8)), // 10
      TransformerApplyNode(10, Seq(1)), // 11
      quadrupleTransformer, // 12 - a no longer used instruction
      TransformerApplyNode(12, Seq(4)), // 13 - a no longer used instruction
      identityEstimator, // 14
      EstimatorFitNode(14, Seq(6)), // 15 - now points at the spliced instructions
      TransformerApplyNode(15, Seq(6)), // 16 - now points at the spliced instructions
      SourceNode(labelData), // 17
      plusDataPlusLabelEstimator, // 18
      EstimatorFitNode(18, Seq(16, 17)), // 19
      quadrupleTransformer, // 20
      TransformerApplyNode(20, Seq(Pipeline.SOURCE)), // 21
      TransformerApplyNode(15, Seq(21)), // 22
      TransformerApplyNode(19, Seq(22)), // 23
      GatherTransformer(), // 24
      TransformerApplyNode(24, Seq(3, 11, 23)) // 25
    ))
  }
}
