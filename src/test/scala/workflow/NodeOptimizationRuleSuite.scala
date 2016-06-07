package workflow

import NodeOptimizationRuleSuite._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines.Logging

import scala.util.Random

class NodeOptimizationRuleSuite extends FunSuite with PipelineContext with Logging {
  test("Test node level optimizations choice some false") {
    PipelineEnv.getOrCreate.setOptimizer(nodeLevelOptOptimizer)
    sc = new SparkContext("local", "test")

    val random = new Random(0)
    // Use random choices to make sure the sample zipping worked correctly
    val choices = Seq.fill(10000)(State(choice = Some(random.nextBoolean())))

    val trainData = sc.parallelize(choices, 12)
    val trainLabels = trainData.map(_.choice.get)

    val pipeline = optimizableTransformer andThen
      (optimizableEstimator, trainData) andThen
      (optimizableLabelEstimator, trainData, trainLabels)

    //val nodeOptimizedPipeline = new NodeOptimizationRule().apply(pipeline)
    val outputState = pipeline.apply(State()).get()

    assert(outputState.transformerChoice.isEmpty, "The optimizable transformer should use the default on test data")
    assert(outputState.estimatorChoice == Some(false), "The optimizable estimator made the incorrect optimization")
    assert(outputState.labelEstimatorChoice == Some(false),
      "The optimizable label estimator made the incorrect optimization")
  }

  test("Test node level optimizations choice all true") {
    PipelineEnv.getOrCreate.setOptimizer(nodeLevelOptOptimizer)
    sc = new SparkContext("local", "test")

    val choices = Seq.fill(10000)(State(choice = Some(true)))

    val trainData = sc.parallelize(choices, 12)
    val trainLabels = trainData.map(_.choice.get)

    val pipeline = optimizableTransformer andThen
      (optimizableEstimator, trainData) andThen
      (optimizableLabelEstimator, trainData, trainLabels)

    val outputState = pipeline(State()).get()

    assert(outputState.transformerChoice.isEmpty, "The optimizable transformer should use the default on test data")
    assert(outputState.estimatorChoice == Some(true), "The optimizable estimator made the incorrect optimization")
    assert(outputState.labelEstimatorChoice == Some(true),
      "The optimizable label estimator made the incorrect optimization")
  }

  test("Test node level optimizations with no opts to make") {
    PipelineEnv.getOrCreate.setOptimizer(nodeLevelOptOptimizer)
    sc = new SparkContext("local", "test")

    val choices = Seq.fill(10000)(State(choice = Some(true)))

    val trainData = sc.parallelize(choices, 12)
    val trainLabels = trainData.map(_.choice.get)

    val pipeline = transformerA andThen
      (estimatorB, trainData) andThen
      (labelEstimatorB, trainData, trainLabels)

    val outputState = pipeline(State()).get()

    assert(outputState === State(None, Some(false), Some(true), Some(true)))
  }

  test("Test node level optimizations with one opt to make") {
    PipelineEnv.getOrCreate.setOptimizer(nodeLevelOptOptimizer)
    sc = new SparkContext("local", "test")

    val choices = Seq.fill(10000)(State(choice = Some(true)))

    val trainData = sc.parallelize(choices, 12)
    val trainLabels = trainData.map(_.choice.get)

    val pipeline = transformerA andThen
      (estimatorB, trainData) andThen
      (optimizableLabelEstimator, trainData, trainLabels)

    val outputState = pipeline(State()).get()

    assert(outputState === State(None, Some(false), Some(true), Some(true)))
  }
}

object NodeOptimizationRuleSuite {
  case class State(
    choice: Option[Boolean] = None,
    transformerChoice: Option[Boolean] = None,
    estimatorChoice: Option[Boolean] = None,
    labelEstimatorChoice: Option[Boolean] = None
  )

  val transformerDoNothing = Transformer[State, State] { x =>
    x.copy(transformerChoice = None)
  }
  val transformerA = Transformer[State, State](_.copy(transformerChoice = Some(false)))
  val transformerB = Transformer[State, State](_.copy(transformerChoice = Some(true)))
  val optimizableTransformer = new OptimizableTransformer[State, State] {
    override val default: Transformer[State, State] = transformerDoNothing
    override def optimize(sample: RDD[State], numPerPartition: Map[Int, Int]): Transformer[State, State] = {
      if (sample.collect().exists(_.choice.get == false)) {
        transformerA
      } else {
        transformerB
      }
    }
  }


  val estimatorDoNothing = new Estimator[State, State] {
    override def fit(data: RDD[State]): Transformer[State, State] = {
      Transformer[State, State](_.copy(estimatorChoice = None))
    }
  }

  val estimatorA = new Estimator[State, State] {
    override def fit(data: RDD[State]): Transformer[State, State] = {
      Transformer[State, State](_.copy(estimatorChoice = Some(false)))
    }
  }

  val estimatorB = new Estimator[State, State] {
    override def fit(data: RDD[State]): Transformer[State, State] = {
      Transformer[State, State](_.copy(estimatorChoice = Some(true)))
    }
  }

  val optimizableEstimator = new OptimizableEstimator[State, State] {
    override val default: Estimator[State, State] = estimatorDoNothing
    override def optimize(sample: RDD[State], numPerPartition: Map[Int, Int]): Estimator[State, State] = {
      if (sample.collect().exists(_.choice.get == false)) {
        estimatorA
      } else {
        estimatorB
      }
    }
  }


  val labelEstimatorDoNothing = new LabelEstimator[State, State, Boolean] {
    override def fit(data: RDD[State], labels: RDD[Boolean]): Transformer[State, State] = {
      Transformer[State, State](_.copy(labelEstimatorChoice = None))
    }
  }

  val labelEstimatorA = new LabelEstimator[State, State, Boolean] {
    override def fit(data: RDD[State], labels: RDD[Boolean]): Transformer[State, State] = {
      Transformer[State, State](_.copy(labelEstimatorChoice = Some(false)))
    }
  }

  val labelEstimatorB = new LabelEstimator[State, State, Boolean] {
    override def fit(data: RDD[State], labels: RDD[Boolean]): Transformer[State, State] = {
      Transformer[State, State](_.copy(labelEstimatorChoice = Some(true)))
    }
  }

  val optimizableLabelEstimator = new OptimizableLabelEstimator[State, State, Boolean] {
    override val default: LabelEstimator[State, State, Boolean] = labelEstimatorDoNothing
    override def optimize(sample: RDD[State], sampleLabels: RDD[Boolean], numPerPartition: Map[Int, Int])
    : LabelEstimator[State, State, Boolean] = {
      // Test to make sure the zipping worked correctly
      sample.zip(sampleLabels).foreach { x =>
        assert(x._1.choice.get == x._2, "Label and choice must be equal!")
      }

      if (sample.collect().exists(_.choice.get == false)) {
        labelEstimatorA
      } else {
        labelEstimatorB
      }
    }
  }

  val nodeLevelOptOptimizer = new Optimizer {
    override protected val batches: Seq[Batch] = Batch("Node Level Optimization", Once, new NodeOptimizationRule) ::
      Nil
  }
}
