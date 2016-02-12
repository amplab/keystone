package workflow

import nodes.util.Cacher
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}

class AutoCacheRuleSuite extends FunSuite with LocalSparkContext with Logging {
  val transformerOne = Transformer[Int, Int](x => x + 1)
  val transformerTwo = Transformer[Int, Int](x => x + 2)
  val transformerThree = Transformer[Int, Int](x => x + 3)
  val transformerFour = Transformer[Int, Int](x => x + 4)
  val transformerFive = Transformer[Int, Int](x => x + 5)
  val transformerOneSource = Transformer[Int, Int](x => x + 1)
  val transformerTwoSource = Transformer[Int, Int](x => x + 2)
  val transformerThreeSource = Transformer[Int, Int](x => x + 3)
  val transformerFourSource = Transformer[Int, Int](x => x + 4)
  val transformerFiveSource = Transformer[Int, Int](x => x + 5)
  val estimator = new Estimator[Int, Int] with WeightedNode {
    override protected def fit(data: RDD[Int]): Transformer[Int, Int] = transformerOne
    override val weight: Int = 4
  }

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
    0 -> Profile(10, Long.MaxValue),
    6 -> Profile(10, 50),
    7 -> Profile(30, 200),
    8 -> Profile(20, 1000),
    9 -> Profile(20, 1000),
    10 -> Profile(20, 100)
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
    val cachedInstructions = autoCacheRule.greedyCache(instructions, profiles, 10)

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
    val cachedInstructions = autoCacheRule.greedyCache(instructions, profiles, 75)

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
    val cachedInstructions = autoCacheRule.greedyCache(instructions, profiles, 125)

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
    val cachedInstructions = autoCacheRule.greedyCache(instructions, profiles, 175)

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
    val cachedInstructions = autoCacheRule.greedyCache(instructions, profiles, 350)

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
    val cachedInstructions = autoCacheRule.greedyCache(instructions, profiles, 10000)

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
