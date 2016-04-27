package internal

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}

class GraphSuite extends FunSuite with LocalSparkContext with Logging {

  val graph = Graph(
    sources = Set(SourceId(1), SourceId(2), SourceId(3)),
    operators = Map(
      NodeId(0) -> DatumOperator(0),
      NodeId(1) -> DatumOperator(1),
      NodeId(2) -> DatumOperator(2),
      NodeId(3) -> DatumOperator(3),
      NodeId(4) -> DatumOperator(4),
      NodeId(5) -> DatumOperator(5),
      NodeId(6) -> DatumOperator(6),
      NodeId(7) -> DatumOperator(7),
      NodeId(8) -> DatumOperator(8),
      NodeId(9) -> DatumOperator(9)
    ),
    dependencies = Map(
      NodeId(0) -> Seq(),
      NodeId(1) -> Seq(SourceId(1), SourceId(2)),
      NodeId(2) -> Seq(),
      NodeId(3) -> Seq(SourceId(3)),
      NodeId(4) -> Seq(NodeId(1), NodeId(2)),
      NodeId(5) -> Seq(NodeId(2), NodeId(3), NodeId(4)),
      NodeId(6) -> Seq(SourceId(3), NodeId(1)),
      NodeId(7) -> Seq(SourceId(1), NodeId(1), NodeId(6)),
      NodeId(8) -> Seq(NodeId(4), NodeId(5)),
      NodeId(9) -> Seq(NodeId(0), NodeId(3), NodeId(7), NodeId(8))
    ),
    sinkDependencies = Map(
      SinkId(0) -> SourceId(2),
      SinkId(1) -> NodeId(4),
      SinkId(2) -> NodeId(9)
    )
  )

  test("nodes") {
    assert(graph.nodes === (0 to 9).map(i => NodeId(i)).toSet)
  }

  test("sinks") {
    assert(graph.sinks === Set(SinkId(0), SinkId(1), SinkId(2)))
  }

  test("getDependencies") {
    assert(graph.getDependencies(NodeId(2)) === Seq())
    assert(graph.getDependencies(NodeId(7)) === Seq(SourceId(1), NodeId(1), NodeId(6)))
    assert(graph.getDependencies(NodeId(9)) === Seq(NodeId(0), NodeId(3), NodeId(7), NodeId(8)))

    intercept[NoSuchElementException] {
      graph.getDependencies(NodeId(10))
    }
  }

  test("getSinkDependency") {
    assert(graph.getSinkDependency(SinkId(0)) === SourceId(2))
    assert(graph.getSinkDependency(SinkId(1)) === NodeId(4))
    assert(graph.getSinkDependency(SinkId(2)) === NodeId(9))

    intercept[NoSuchElementException] {
      graph.getSinkDependency(SinkId(10))
    }
  }

  test("getOperator") {
    assert(graph.getOperator(NodeId(2)) === DatumOperator(2))
    assert(graph.getOperator(NodeId(7)) === DatumOperator(7))
    assert(graph.getOperator(NodeId(9)) === DatumOperator(9))

    intercept[NoSuchElementException] {
      graph.getOperator(NodeId(10))
    }
  }

  test("addNode") {
    intercept[IllegalArgumentException] {
      graph.addNode(DatumOperator(10), Seq(NodeId(11), SourceId(2)))
    }

    intercept[IllegalArgumentException] {
      graph.addNode(DatumOperator(10), Seq(NodeId(7), SourceId(11)))
    }

    val (newGraph, newId) = graph.addNode(DatumOperator(10), Seq(NodeId(7), SourceId(1)))

    assert(!graph.nodes.contains(newId), "New node id must not collide with existing ids in graph")

    val expectedGraph = graph.copy(
      operators = graph.operators + (newId -> DatumOperator(10)),
      dependencies = graph.dependencies + (newId -> Seq(NodeId(7), SourceId(1)))
    )

    assert(expectedGraph === newGraph)
  }

  test("addSink") {
    intercept[IllegalArgumentException] {
      graph.addSink(NodeId(11))
    }

    intercept[IllegalArgumentException] {
      graph.addSink(SourceId(11))
    }

    // Test adding a sink on a node
    {
      val (newGraph, newId) = graph.addSink(NodeId(7))
      assert(!graph.sinks.contains(newId), "New sink id must not collide with existing ids in graph")

      val expectedGraph = graph.copy(
        sinkDependencies = graph.sinkDependencies + (newId -> NodeId(7))
      )

      assert(expectedGraph === newGraph)
    }

    // Test adding a sink on a source
    {
      val (newGraph, newId) = graph.addSink(SourceId(2))
      assert(!graph.sinks.contains(newId), "New sink id must not collide with existing ids in graph")

      val expectedGraph = graph.copy(
        sinkDependencies = graph.sinkDependencies + (newId -> SourceId(2))
      )

      assert(expectedGraph === newGraph)
    }

  }

  test("addSource") {
    val (newGraph, newId) = graph.addSource()

    assert(!graph.sources.contains(newId), "New source id must not collide with existing ids in graph")

    val expectedGraph = Graph(
      sources = graph.sources + newId,
      operators = graph.operators,
      dependencies = graph.dependencies,
      sinkDependencies = graph.sinkDependencies
    )

    assert(expectedGraph === newGraph)
  }

  test("setDependencies") {
    intercept[IllegalArgumentException] {
      graph.setDependencies(NodeId(13), Seq(NodeId(4), SourceId(1)))
    }

    intercept[IllegalArgumentException] {
      graph.setDependencies(NodeId(7), Seq(NodeId(11), SourceId(2)))
    }

    intercept[IllegalArgumentException] {
      graph.setDependencies(NodeId(7), Seq(NodeId(3), SourceId(11)))
    }

    val newGraph = graph.setDependencies(NodeId(8), Seq(NodeId(7), SourceId(1)))

    val expectedGraph = graph.copy(
      dependencies = graph.dependencies + (NodeId(8) -> Seq(NodeId(7), SourceId(1)))
    )

    assert(expectedGraph === newGraph)
  }

  test("setOperator") {
    intercept[IllegalArgumentException] {
      graph.setOperator(NodeId(13), DatumOperator(13))
    }

    val newGraph = graph.setOperator(NodeId(7), DatumOperator(13))

    val expectedGraph = graph.copy(
      operators = graph.operators + (NodeId(7) -> DatumOperator(13))
    )

    assert(expectedGraph === newGraph)
  }

  test("setSinkDependency") {
    intercept[IllegalArgumentException] {
      graph.setSinkDependency(SinkId(13), SourceId(1))
    }

    intercept[IllegalArgumentException] {
      graph.setSinkDependency(SinkId(2), SourceId(-10))
    }

    intercept[IllegalArgumentException] {
      graph.setSinkDependency(SinkId(2), NodeId(-10))
    }

    val newGraph = graph.setSinkDependency(SinkId(2), NodeId(7))

    val expectedGraph = graph.copy(
      sinkDependencies = graph.sinkDependencies + (SinkId(2) -> NodeId(7))
    )

    assert(expectedGraph === newGraph)
  }

  test("removeSink") {
    intercept[IllegalArgumentException] {
      graph.removeSink(SinkId(13))
    }

    val newGraph = graph.removeSink(SinkId(2))

    val expectedGraph = graph.copy(
      sinkDependencies = graph.sinkDependencies - SinkId(2)
    )

    assert(expectedGraph === newGraph)
  }

  test("removeSource") {
    intercept[IllegalArgumentException] {
      graph.removeSource(SourceId(13))
    }

    val newGraph = graph.removeSource(SourceId(2))

    val expectedGraph = graph.copy(
      sources = graph.sources - SourceId(2)
    )

    assert(expectedGraph === newGraph)
  }

  test("removeNode") {
    intercept[IllegalArgumentException] {
      graph.removeNode(NodeId(13))
    }

    val newGraph = graph.removeNode(NodeId(5))

    val expectedGraph = graph.copy(
      operators = graph.operators - NodeId(5),
      dependencies = graph.dependencies - NodeId(5)
    )

    assert(expectedGraph === newGraph)
  }

  test("replaceDependency") {
    // Intercept whenever trying to insert a non-existant id
    {
      intercept[IllegalArgumentException] {
        graph.replaceDependency(NodeId(1), SourceId(-5))
      }

      intercept[IllegalArgumentException] {
        graph.replaceDependency(NodeId(1), NodeId(-5))
      }
    }

    // Replace a source with a node
    {
      val newGraph = graph.replaceDependency(SourceId(2), NodeId(3))

      val expectedGraph = graph.copy(

        dependencies = Map(
          NodeId(0) -> Seq(),
          NodeId(1) -> Seq(SourceId(1), NodeId(3)),
          NodeId(2) -> Seq(),
          NodeId(3) -> Seq(SourceId(3)),
          NodeId(4) -> Seq(NodeId(1), NodeId(2)),
          NodeId(5) -> Seq(NodeId(2), NodeId(3), NodeId(4)),
          NodeId(6) -> Seq(SourceId(3), NodeId(1)),
          NodeId(7) -> Seq(SourceId(1), NodeId(1), NodeId(6)),
          NodeId(8) -> Seq(NodeId(4), NodeId(5)),
          NodeId(9) -> Seq(NodeId(0), NodeId(3), NodeId(7), NodeId(8))
        ),
        sinkDependencies = Map(
          SinkId(0) -> NodeId(3),
          SinkId(1) -> NodeId(4),
          SinkId(2) -> NodeId(9)
        )
      )

      assert(expectedGraph === newGraph)
    }

    // Replace a source with a source
    {
      val newGraph = graph.replaceDependency(SourceId(2), SourceId(1))

      val expectedGraph = graph.copy(
        dependencies = Map(
          NodeId(0) -> Seq(),
          NodeId(1) -> Seq(SourceId(1), SourceId(1)),
          NodeId(2) -> Seq(),
          NodeId(3) -> Seq(SourceId(3)),
          NodeId(4) -> Seq(NodeId(1), NodeId(2)),
          NodeId(5) -> Seq(NodeId(2), NodeId(3), NodeId(4)),
          NodeId(6) -> Seq(SourceId(3), NodeId(1)),
          NodeId(7) -> Seq(SourceId(1), NodeId(1), NodeId(6)),
          NodeId(8) -> Seq(NodeId(4), NodeId(5)),
          NodeId(9) -> Seq(NodeId(0), NodeId(3), NodeId(7), NodeId(8))
        ),
        sinkDependencies = Map(
          SinkId(0) -> SourceId(1),
          SinkId(1) -> NodeId(4),
          SinkId(2) -> NodeId(9)
        )
      )

      assert(expectedGraph === newGraph)
    }

    // Replace a node with a source
    {
      val newGraph = graph.replaceDependency(NodeId(3), SourceId(3))

      val expectedGraph = graph.copy(
        dependencies = Map(
          NodeId(0) -> Seq(),
          NodeId(1) -> Seq(SourceId(1), SourceId(2)),
          NodeId(2) -> Seq(),
          NodeId(3) -> Seq(SourceId(3)),
          NodeId(4) -> Seq(NodeId(1), NodeId(2)),
          NodeId(5) -> Seq(NodeId(2), SourceId(3), NodeId(4)),
          NodeId(6) -> Seq(SourceId(3), NodeId(1)),
          NodeId(7) -> Seq(SourceId(1), NodeId(1), NodeId(6)),
          NodeId(8) -> Seq(NodeId(4), NodeId(5)),
          NodeId(9) -> Seq(NodeId(0), SourceId(3), NodeId(7), NodeId(8))
        ),
        sinkDependencies = Map(
          SinkId(0) -> SourceId(2),
          SinkId(1) -> NodeId(4),
          SinkId(2) -> NodeId(9)
        )
      )

      assert(expectedGraph === newGraph)
    }

    // Replace a node with a node
    {
      val newGraph = graph.replaceDependency(NodeId(4), NodeId(2))

      val expectedGraph = graph.copy(
        dependencies = Map(
          NodeId(0) -> Seq(),
          NodeId(1) -> Seq(SourceId(1), SourceId(2)),
          NodeId(2) -> Seq(),
          NodeId(3) -> Seq(SourceId(3)),
          NodeId(4) -> Seq(NodeId(1), NodeId(2)),
          NodeId(5) -> Seq(NodeId(2), NodeId(3), NodeId(2)),
          NodeId(6) -> Seq(SourceId(3), NodeId(1)),
          NodeId(7) -> Seq(SourceId(1), NodeId(1), NodeId(6)),
          NodeId(8) -> Seq(NodeId(2), NodeId(5)),
          NodeId(9) -> Seq(NodeId(0), NodeId(3), NodeId(7), NodeId(8))
        ),
        sinkDependencies = Map(
          SinkId(0) -> SourceId(2),
          SinkId(1) -> NodeId(2),
          SinkId(2) -> NodeId(9)
        )
      )

      assert(expectedGraph === newGraph)
    }
  }

  test("addGraph") {
    sc = new SparkContext("local", "test")

    require(false)
  }

  test("connectGraph") {
    sc = new SparkContext("local", "test")

    require(false)
  }

  test("replaceNodes") {
    sc = new SparkContext("local", "test")

    require(false)
  }

}
