package workflow.graph

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}
import workflow.AutoCacheRule.GreedyCache

case class Waiter(time: Long, nanos: Int = 0) {
  def waitHere(): Unit = synchronized {
    wait(time, nanos)
  }
}

case class TransformerPlus(plus: Int, time: Int) extends Transformer[Int, Int] {
  override def apply(in: Int): Int = {
    Waiter(time).waitHere()
    in + plus
  }
}

class AutoCacheRuleSuite extends FunSuite with LocalSparkContext with Logging {

  def estimatorOne = new Estimator[Int, Int] with WeightedOperator {
    override def fit(data: RDD[Int]): Transformer[Int, Int] = {
      synchronized {
        wait(10)
      }
      val sums = data.collect()
      data.collect().foreach(_ => wait(0, 100))
      TransformerPlus(sums.sum, 100)
    }

    override val weight: Int = 2
  }

  def estimatorTwo = new Estimator[Int, Int] with WeightedOperator {
    override def fit(data: RDD[Int]): Transformer[Int, Int] = {
      synchronized {
        wait(2)
      }
      data.collect()
      val sums = data.collect()
      data.collect()
      data.collect().foreach(_ => wait(0, 200))
      TransformerPlus(sums.sum, 200)
    }

    override val weight: Int = 4
  }

  def getPlan(sc: SparkContext): Graph = {
    val train = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8))
    Graph(
      sources = Set(SourceId(0)),
      operators = Map(
        NodeId(0) -> DatasetOperator(train),
        NodeId(1) -> TransformerPlus(1, 50),
        NodeId(2) -> TransformerPlus(2, 100),
        NodeId(3) -> TransformerPlus(3, 150),
        NodeId(4) -> TransformerPlus(4, 200),
        NodeId(5) -> TransformerPlus(5, 250),
        NodeId(6) -> estimatorOne,
        NodeId(7) -> new DelegatingOperator,
        NodeId(8) -> TransformerPlus(1, 50),
        NodeId(9) -> TransformerPlus(2, 100),
        NodeId(10) -> TransformerPlus(3, 150),
        NodeId(11) -> TransformerPlus(4, 200),
        NodeId(12) -> TransformerPlus(5, 250)
      ),
      dependencies = Map(
        NodeId(0) -> Seq(),
        NodeId(1) -> Seq(NodeId(0)),
        NodeId(2) -> Seq(NodeId(1)),
        NodeId(3) -> Seq(NodeId(2)),
        NodeId(4) -> Seq(NodeId(2)),
        NodeId(5) -> Seq(NodeId(3), NodeId(4)),
        NodeId(6) -> Seq(NodeId(5)),
        NodeId(7) -> Seq(NodeId(6), NodeId(12)),
        NodeId(8) -> Seq(SourceId(0)),
        NodeId(9) -> Seq(NodeId(8)),
        NodeId(10) -> Seq(NodeId(9)),
        NodeId(11) -> Seq(NodeId(9)),
        NodeId(12) -> Seq(NodeId(10), NodeId(11))
      ),
      sinkDependencies = Map(SinkId(0) -> NodeId(7))
    )
  }

  // TODO: Profiling tests
  test("pipeline chaining") {
    // Clean global pipeline state
    sc = new SparkContext("local", "test")
    val plan = getPlan(sc)
    val rule = new AutoCacheRule(GreedyCache())
    rule.apply(plan, Map())
  }

  // TODO: cache insertion tests

  // TODO: end to end?
}
/*
val instructions = Seq[Instruction] (
  SourceNode(null), // 0
  transformerOne, // 1
  transformerTwo, // 2
  transformerThree, // 3
  transformerFour, // 4
  transformerFive, // 5
  TransformerApplyNode(1, Seq(0)), // 6
  TransformerApplyNode(2, Seq(6)), // 7
  TransformerApplyNode(3, Seq(7)), // 8
  TransformerApplyNode(4, Seq(7)), // 9
  TransformerApplyNode(5, Seq(8, 9)), // 10
  estimator, // 11
  EstimatorFitNode(11, Seq(10)), // 12
  // Now alter Pipeline.Source
  transformerOneSource, // 13
  transformerTwoSource, // 14
  transformerThreeSource, // 15
  transformerFourSource, // 16
  transformerFiveSource, // 17
  TransformerApplyNode(13, Seq(Pipeline.SOURCE)), // 18
  TransformerApplyNode(14, Seq(18)), // 19
  TransformerApplyNode(15, Seq(19)), // 20
  TransformerApplyNode(16, Seq(19)), // 21
  TransformerApplyNode(17, Seq(20, 21)), // 22
  TransformerApplyNode(12, Seq(22)) // 22
)

val profiles = Map[Int, Profile](
  0 -> Profile(10, Long.MaxValue, 0),
  6 -> Profile(10, 50, 0),
  7 -> Profile(30, 200, 0),
  8 -> Profile(20, 1000, 0),
  9 -> Profile(20, 1000, 0),
  10 -> Profile(20, 100, 0)
)

test("Naive cacher") {
  val autoCacheRule = new AutoCacheRule(null)
  val cachedInstructions = autoCacheRule.aggressiveCache(instructions)

  val cachedTransformers = cachedInstructions.collect {
    case TransformerApplyNode(maybeCacher, inputs) if cachedInstructions(maybeCacher).isInstanceOf[Cacher[_]] =>
      cachedInstructions(inputs.head) match {
        case TransformerApplyNode(transformer, _) => cachedInstructions(transformer)
        case _ => throw new RuntimeException("Unexpected cached node")
      }
  }.toSet

  assert(cachedTransformers === Set(transformerTwo, transformerFive))
}

test("Greedy cacher, max mem 10") {
  val autoCacheRule = new AutoCacheRule(null)
  val cachedInstructions = autoCacheRule.greedyCache(instructions, profiles, Some(10))

  val cachedTransformers = cachedInstructions.collect {
    case TransformerApplyNode(maybeCacher, inputs) if cachedInstructions(maybeCacher).isInstanceOf[Cacher[_]] =>
      cachedInstructions(inputs.head) match {
        case TransformerApplyNode(transformer, _) => cachedInstructions(transformer)
        case _ => throw new RuntimeException("Unexpected cached node")
      }
  }.toSet

  assert(cachedTransformers === Set())
}

test("Greedy cacher, max mem 75") {
  val autoCacheRule = new AutoCacheRule(null)
  val cachedInstructions = autoCacheRule.greedyCache(instructions, profiles, Some(75))

  val cachedTransformers = cachedInstructions.collect {
    case TransformerApplyNode(maybeCacher, inputs) if cachedInstructions(maybeCacher).isInstanceOf[Cacher[_]] =>
      cachedInstructions(inputs.head) match {
        case TransformerApplyNode(transformer, _) => cachedInstructions(transformer)
        case _ => throw new RuntimeException("Unexpected cached node")
      }
  }.toSet

  assert(cachedTransformers === Set(transformerOne))
}

test("Greedy cacher, max mem 125") {
  val autoCacheRule = new AutoCacheRule(null)
  val cachedInstructions = autoCacheRule.greedyCache(instructions, profiles, Some(125))

  val cachedTransformers = cachedInstructions.collect {
    case TransformerApplyNode(maybeCacher, inputs) if cachedInstructions(maybeCacher).isInstanceOf[Cacher[_]] =>
      cachedInstructions(inputs.head) match {
        case TransformerApplyNode(transformer, _) => cachedInstructions(transformer)
        case _ => throw new RuntimeException("Unexpected cached node")
      }
  }.toSet

  assert(cachedTransformers === Set(transformerFive))
}

test("Greedy cacher, max mem 175") {
  val autoCacheRule = new AutoCacheRule(null)
  val cachedInstructions = autoCacheRule.greedyCache(instructions, profiles, Some(175))

  val cachedTransformers = cachedInstructions.collect {
    case TransformerApplyNode(maybeCacher, inputs) if cachedInstructions(maybeCacher).isInstanceOf[Cacher[_]] =>
      cachedInstructions(inputs.head) match {
        case TransformerApplyNode(transformer, _) => cachedInstructions(transformer)
        case _ => throw new RuntimeException("Unexpected cached node")
      }
  }.toSet

  assert(cachedTransformers === Set(transformerOne, transformerFive))
}

test("Greedy cacher, max mem 350") {
  val autoCacheRule = new AutoCacheRule(null)
  val cachedInstructions = autoCacheRule.greedyCache(instructions, profiles, Some(350))

  val cachedTransformers = cachedInstructions.collect {
    case TransformerApplyNode(maybeCacher, inputs) if cachedInstructions(maybeCacher).isInstanceOf[Cacher[_]] =>
      cachedInstructions(inputs.head) match {
        case TransformerApplyNode(transformer, _) => cachedInstructions(transformer)
        case _ => throw new RuntimeException("Unexpected cached node")
      }
  }.toSet

  assert(cachedTransformers === Set(transformerTwo, transformerFive))
}

test("Greedy cacher, max mem 10000") {
  val autoCacheRule = new AutoCacheRule(null)
  val cachedInstructions = autoCacheRule.greedyCache(instructions, profiles, Some(10000))

  val cachedTransformers = cachedInstructions.collect {
    case TransformerApplyNode(maybeCacher, inputs) if cachedInstructions(maybeCacher).isInstanceOf[Cacher[_]] =>
      cachedInstructions(inputs.head) match {
        case TransformerApplyNode(transformer, _) => cachedInstructions(transformer)
        case _ => throw new RuntimeException("Unexpected cached node")
      }
  }.toSet

  assert(cachedTransformers === Set(transformerTwo, transformerFive))
}
}
*/