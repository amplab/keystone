package workflow

import NodeOptimizationRuleSuite._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}

import scala.util.Random

class NodeOptimizationRuleSuite extends FunSuite with LocalSparkContext with Logging {
  test("Test node level optimizations choice some false") {
    sc = new SparkContext("local", "test")

    val random = new Random(0)
    // Use random choices to make sure the sample zipping worked correctly
    val choices = Seq.fill(10000)(State(choice = Some(random.nextBoolean())))

    val trainData = sc.parallelize(choices, 12)
    val trainLabels = trainData.map(_.choice.get)

    val pipeline = optimizableTransformer andThen
      (optimizableEstimator, trainData) andThen
      (optimizableLabelEstimator, trainData, trainLabels)

    val nodeOptimizedPipeline = new NodeOptimizationRule(0.01).apply(pipeline)
    val outputState = nodeOptimizedPipeline.apply(State(), optimizer = None)

    assert(outputState.transformerChoice.isEmpty, "The optimizable transformer should use the default on test data")
    assert(outputState.estimatorChoice == Some(false), "The optimizable estimator made the incorrect optimization")
    assert(outputState.labelEstimatorChoice == Some(false),
      "The optimizable label estimator made the incorrect optimization")
  }

  test("Test node level optimizations choice all true") {
    sc = new SparkContext("local", "test")

    val choices = Seq.fill(10000)(State(choice = Some(true)))

    val trainData = sc.parallelize(choices, 12)
    val trainLabels = trainData.map(_.choice.get)

    val pipeline = optimizableTransformer andThen
      (optimizableEstimator, trainData) andThen
      (optimizableLabelEstimator, trainData, trainLabels)

    val nodeOptimizedPipeline = new NodeOptimizationRule(0.01).apply(pipeline)
    val outputState = nodeOptimizedPipeline.apply(State(), optimizer = None)

    assert(outputState.transformerChoice.isEmpty, "The optimizable transformer should use the default on test data")
    assert(outputState.estimatorChoice == Some(true), "The optimizable estimator made the incorrect optimization")
    assert(outputState.labelEstimatorChoice == Some(true),
      "The optimizable label estimator made the incorrect optimization")
  }

  test("Test node level optimizations with no opts to make") {
    sc = new SparkContext("local", "test")

    val choices = Seq.fill(10000)(State(choice = Some(true)))

    val trainData = sc.parallelize(choices, 12)
    val trainLabels = trainData.map(_.choice.get)

    val pipeline = transformerA andThen
      (estimatorB, trainData) andThen
      (labelEstimatorB, trainData, trainLabels)

    val nodeOptimizedPipeline = new NodeOptimizationRule(0.01).apply(pipeline)
    val outputState = nodeOptimizedPipeline.apply(State(), optimizer = None)

    assert(outputState === State(None, Some(false), Some(true), Some(true)))
  }

  test("Test node level optimizations with one opt to make") {
    sc = new SparkContext("local", "test")

    val choices = Seq.fill(10000)(State(choice = Some(true)))

    val trainData = sc.parallelize(choices, 12)
    val trainLabels = trainData.map(_.choice.get)

    val pipeline = transformerA andThen
      (estimatorB, trainData) andThen
      (optimizableLabelEstimator, trainData, trainLabels)

    val nodeOptimizedPipeline = new NodeOptimizationRule(0.01).apply(pipeline)
    val outputState = nodeOptimizedPipeline.apply(State(), optimizer = None)

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
    assert(x.choice.isEmpty, "The default transformer must only be used on test data")
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


  val estimatorDoNothing = Estimator[State, State] {
    x => Transformer[State, State](_.copy(estimatorChoice = None))
  }
  val estimatorA = Estimator[State, State] {
    x => Transformer[State, State](_.copy(estimatorChoice = Some(false)))
  }
  val estimatorB = Estimator[State, State] {
    x => Transformer[State, State](_.copy(estimatorChoice = Some(true)))
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


  val labelEstimatorDoNothing = LabelEstimator[State, State, Boolean] {
    case (a, b) => Transformer[State, State](_.copy(labelEstimatorChoice = None))
  }
  val labelEstimatorA = LabelEstimator[State, State, Boolean] {
    case (a, b) => Transformer[State, State](_.copy(labelEstimatorChoice = Some(false)))
  }
  val labelEstimatorB = LabelEstimator[State, State, Boolean] {
    case (a, b) => Transformer[State, State](_.copy(labelEstimatorChoice = Some(true)))
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
}
