package workflow

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}

class WorkflowUtilsSuite extends FunSuite with LocalSparkContext with Logging {
  test("Pipeline to Instructions to Pipeline") {
    sc = new SparkContext("local", "test")

    val trainData = sc.parallelize(Seq(32, 94, 12))

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

    val firstPipeline = doubleTransformer andThen minusThreeTransformer

    val plusDataEstimator = new Estimator[Int, Int] {
      protected def fit(data: RDD[Int]): Transformer[Int, Int] = {
        val first = data.first()
        Transformer(x => x + first)
      }
      override val label = "x + data.first()"
    }
    val secondPipeline = doubleTransformer andThen (plusDataEstimator, trainData)

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
    val labelData = sc.parallelize(Seq("10", "7", "14"))
    val thirdPipeline = quadrupleTransformer andThen
      (identityEstimator, trainData) andThen
      (plusDataPlusLabelEstimator, trainData, labelData)

    val initialPipeline = new Optimizer {
      protected val batches: Seq[Batch] = Batch("DAG Optimization", FixedPoint(100), EquivalentNodeMerger) :: Nil
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
}
