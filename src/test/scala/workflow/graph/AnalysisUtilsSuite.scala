package workflow.graph

import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}

class AnalysisUtilsSuite extends FunSuite with LocalSparkContext with Logging {
  val graph = Graph(
    sources = Set(SourceId(1), SourceId(2), SourceId(3)),
    operators = Map(),
    dependencies = Map(
      NodeId(0) -> Seq(),
      NodeId(1) -> Seq(SourceId(1), SourceId(2)),
      NodeId(2) -> Seq(),
      NodeId(3) -> Seq(SourceId(3)),
      NodeId(4) -> Seq(NodeId(1), NodeId(2)),
      NodeId(5) -> Seq(NodeId(2), NodeId(3), NodeId(4)),
      NodeId(6) -> Seq(SourceId(3), NodeId(1)),
      NodeId(7) -> Seq(SourceId(1), NodeId(1)),
      NodeId(8) -> Seq(NodeId(4), NodeId(5)),
      NodeId(9) -> Seq(NodeId(0), NodeId(3), NodeId(8)),
      NodeId(10) -> Seq(NodeId(1), NodeId(6)),
      NodeId(11) -> Seq(NodeId(5), NodeId(8), NodeId(9)),
      NodeId(12) -> Seq(SourceId(2), NodeId(4), NodeId(5), NodeId(6)),
      NodeId(13) -> Seq(NodeId(9), NodeId(10)),
      NodeId(14) -> Seq(NodeId(9), NodeId(10)),
      NodeId(15) -> Seq(NodeId(11), NodeId(12), NodeId(14)),
      NodeId(16) -> Seq(NodeId(7), NodeId(15)),
      NodeId(17) -> Seq(NodeId(7), NodeId(15)),
      NodeId(18) -> Seq(NodeId(14), NodeId(15), NodeId(16), NodeId(17))
    ),
    sinkDependencies = Map(
      SinkId(0) -> SourceId(2),
      SinkId(1) -> NodeId(4),
      SinkId(2) -> NodeId(13),
      SinkId(4) -> NodeId(18)
    )
  )

  test("getChildren") {
    val childrenOfSource1 = Set(NodeId(1), NodeId(7))
    assert(AnalysisUtils.getChildren(graph, SourceId(1)) === childrenOfSource1)
    val childrenOfSource2 = Set(NodeId(1), NodeId(12), SinkId(0))
    assert(AnalysisUtils.getChildren(graph, SourceId(2)) === childrenOfSource2)
    val childrenOfSource3 = Set(NodeId(3), NodeId(6))
    assert(AnalysisUtils.getChildren(graph, SourceId(3)) === childrenOfSource3)

    val childrenOfNode0 = Set(NodeId(9))
    assert(AnalysisUtils.getChildren(graph, NodeId(0)) === childrenOfNode0)
    val childrenOfNode8 = Set(NodeId(9), NodeId(11))
    assert(AnalysisUtils.getChildren(graph, NodeId(8)) === childrenOfNode8)
    val childrenOfNode12 = Set(NodeId(15))
    assert(AnalysisUtils.getChildren(graph, NodeId(12)) === childrenOfNode12)
    val childrenOfNode14 = Set(NodeId(15), NodeId(18))
    assert(AnalysisUtils.getChildren(graph, NodeId(14)) === childrenOfNode14)

    val childrenOfSink0 = Set()
    assert(AnalysisUtils.getChildren(graph, SinkId(0)) === childrenOfSink0)
    val childrenOfSink2 = Set()
    assert(AnalysisUtils.getChildren(graph, SinkId(2)) === childrenOfSink2)
  }

  test("getDescendents") {
    val descendentsOfSource1 = Set(
      NodeId(1),
      NodeId(4),
      NodeId(5),
      NodeId(6),
      NodeId(7),
      NodeId(8),
      NodeId(9),
      NodeId(10),
      NodeId(11),
      NodeId(12),
      NodeId(13),
      NodeId(14),
      NodeId(15),
      NodeId(16),
      NodeId(17),
      NodeId(18),
      SinkId(1),
      SinkId(2),
      SinkId(4))
    assert(AnalysisUtils.getDescendants(graph, SourceId(1)) === descendentsOfSource1)
    val descendentsOfSource2 = Set(
      NodeId(1),
      NodeId(4),
      NodeId(5),
      NodeId(6),
      NodeId(7),
      NodeId(8),
      NodeId(9),
      NodeId(10),
      NodeId(11),
      NodeId(12),
      NodeId(13),
      NodeId(14),
      NodeId(15),
      NodeId(16),
      NodeId(17),
      NodeId(18),
      SinkId(0),
      SinkId(1),
      SinkId(2),
      SinkId(4))
    assert(AnalysisUtils.getDescendants(graph, SourceId(2)) === descendentsOfSource2)
    val descendentsOfSource3 = Set(
      NodeId(3),
      NodeId(5),
      NodeId(6),
      NodeId(8),
      NodeId(9),
      NodeId(10),
      NodeId(11),
      NodeId(12),
      NodeId(13),
      NodeId(14),
      NodeId(15),
      NodeId(16),
      NodeId(17),
      NodeId(18),
      SinkId(2),
      SinkId(4))
    assert(AnalysisUtils.getDescendants(graph, SourceId(3)) === descendentsOfSource3)

    val descendentsOfNode0 = Set(
      NodeId(9),
      NodeId(11),
      NodeId(13),
      NodeId(14),
      NodeId(15),
      NodeId(16),
      NodeId(17),
      NodeId(18),
      SinkId(2),
      SinkId(4))
    assert(AnalysisUtils.getDescendants(graph, NodeId(0)) === descendentsOfNode0)
    val descendentsOfNode8 = Set(
      NodeId(9),
      NodeId(11),
      NodeId(13),
      NodeId(14),
      NodeId(15),
      NodeId(16),
      NodeId(17),
      NodeId(18),
      SinkId(2),
      SinkId(4))
    assert(AnalysisUtils.getDescendants(graph, NodeId(8)) === descendentsOfNode8)
    val descendentsOfNode12 = Set(
      NodeId(15),
      NodeId(16),
      NodeId(17),
      NodeId(18),
      SinkId(4))
    assert(AnalysisUtils.getDescendants(graph, NodeId(12)) === descendentsOfNode12)
    val descendentsOfNode14 = Set(
      NodeId(15),
      NodeId(16),
      NodeId(17),
      NodeId(18),
      SinkId(4))
    assert(AnalysisUtils.getDescendants(graph, NodeId(14)) === descendentsOfNode14)

    val descendentsOfSink0 = Set()
    assert(AnalysisUtils.getDescendants(graph, SinkId(0)) === descendentsOfSink0)
    val descendentsOfSink1 = Set()
    assert(AnalysisUtils.getDescendants(graph, SinkId(1)) === descendentsOfSink1)
    val descendentsOfSink2 = Set()
    assert(AnalysisUtils.getDescendants(graph, SinkId(2)) === descendentsOfSink2)
    val descendentsOfSink4 = Set()
    assert(AnalysisUtils.getDescendants(graph, SinkId(4)) === descendentsOfSink4)
  }

  test("getParents") {
    val parentsOfSource2 = Set()
    assert(AnalysisUtils.getParents(graph, SourceId(2)) === parentsOfSource2)

    val parentsOfNode0 = Set()
    assert(AnalysisUtils.getParents(graph, NodeId(0)) === parentsOfNode0)
    val parentsOfNode8 = Set(NodeId(4), NodeId(5))
    assert(AnalysisUtils.getParents(graph, NodeId(8)) === parentsOfNode8)
    val parentsOfNode12 = Set(SourceId(2), NodeId(4), NodeId(5), NodeId(6))
    assert(AnalysisUtils.getParents(graph, NodeId(12)) === parentsOfNode12)
    val parentsOfNode14 = Set(NodeId(9), NodeId(10))
    assert(AnalysisUtils.getParents(graph, NodeId(14)) === parentsOfNode14)

    val parentsOfSink0 = Set(SourceId(2))
    assert(AnalysisUtils.getParents(graph, SinkId(0)) === parentsOfSink0)
    val parentsOfSink2 = Set(NodeId(13))
    assert(AnalysisUtils.getParents(graph, SinkId(2)) === parentsOfSink2)
  }

  test("getAncestors") {
    val ancestorsOfSource2 = Set()
    assert(AnalysisUtils.getAncestors(graph, SourceId(2)) === ancestorsOfSource2)

    val ancestorsOfNode0 = Set()
    assert(AnalysisUtils.getAncestors(graph, NodeId(0)) === ancestorsOfNode0)
    val ancestorsOfNode8 = Set(
      SourceId(1),
      SourceId(2),
      SourceId(3),
      NodeId(1),
      NodeId(2),
      NodeId(3),
      NodeId(4),
      NodeId(5))
    assert(AnalysisUtils.getAncestors(graph, NodeId(8)) === ancestorsOfNode8)
    val ancestorsOfNode12 = Set(
      SourceId(1),
      SourceId(2),
      SourceId(3),
      NodeId(1),
      NodeId(2),
      NodeId(3),
      NodeId(4),
      NodeId(5),
      NodeId(6))
    assert(AnalysisUtils.getAncestors(graph, NodeId(12)) === ancestorsOfNode12)
    val ancestorsOfNode14 = Set(
      SourceId(1),
      SourceId(2),
      SourceId(3),
      NodeId(0),
      NodeId(1),
      NodeId(2),
      NodeId(3),
      NodeId(4),
      NodeId(5),
      NodeId(6),
      NodeId(8),
      NodeId(9),
      NodeId(10))
    assert(AnalysisUtils.getAncestors(graph, NodeId(14)) === ancestorsOfNode14)

    val ancestorsOfSink0 = Set(SourceId(2))
    assert(AnalysisUtils.getAncestors(graph, SinkId(0)) === ancestorsOfSink0)
    val ancestorsOfSink1 = Set(
      SourceId(1),
      SourceId(2),
      NodeId(1),
      NodeId(2),
      NodeId(4))
    assert(AnalysisUtils.getAncestors(graph, SinkId(1)) === ancestorsOfSink1)
    val ancestorsOfSink2 = Set(
      SourceId(1),
      SourceId(2),
      SourceId(3),
      NodeId(0),
      NodeId(1),
      NodeId(2),
      NodeId(3),
      NodeId(4),
      NodeId(5),
      NodeId(6),
      NodeId(8),
      NodeId(9),
      NodeId(10),
      NodeId(13))
    assert(AnalysisUtils.getAncestors(graph, SinkId(2)) === ancestorsOfSink2)
    val ancestorsOfSink4 = Set(
      SourceId(1),
      SourceId(2),
      SourceId(3),
      NodeId(0),
      NodeId(1),
      NodeId(2),
      NodeId(3),
      NodeId(4),
      NodeId(5),
      NodeId(6),
      NodeId(7),
      NodeId(8),
      NodeId(9),
      NodeId(10),
      NodeId(11),
      NodeId(12),
      // Notice: No NodeId(13)
      NodeId(14),
      NodeId(15),
      NodeId(16),
      NodeId(17),
      NodeId(18))
    assert(AnalysisUtils.getAncestors(graph, SinkId(4)) === ancestorsOfSink4)
  }

  test("linearize") {
    val linearizationAtSource2 = Seq()
    assert(AnalysisUtils.linearize(graph, SourceId(2)) === linearizationAtSource2)

    val linearizationAtNode0 = Seq()
    assert(AnalysisUtils.linearize(graph, NodeId(0)) === linearizationAtNode0)
    val linearizationAtNode8 = Seq(
      SourceId(1),
      SourceId(2),
      NodeId(1),
      NodeId(2),
      NodeId(4),
      SourceId(3),
      NodeId(3),
      NodeId(5))
    assert(AnalysisUtils.linearize(graph, NodeId(8)) === linearizationAtNode8)
    val linearizationAtNode12 = Seq(
      SourceId(2),
      SourceId(1),
      NodeId(1),
      NodeId(2),
      NodeId(4),
      SourceId(3),
      NodeId(3),
      NodeId(5),
      NodeId(6))
    assert(AnalysisUtils.linearize(graph, NodeId(12)) === linearizationAtNode12)
    val linearizationAtNode14 = Seq(
      NodeId(0),
      SourceId(3),
      NodeId(3),
      SourceId(1),
      SourceId(2),
      NodeId(1),
      NodeId(2),
      NodeId(4),
      NodeId(5),
      NodeId(8),
      NodeId(9),
      NodeId(6),
      NodeId(10))
    assert(AnalysisUtils.linearize(graph, NodeId(14)) === linearizationAtNode14)

    val linearizationAtSink0 = Seq(SourceId(2))
    assert(AnalysisUtils.linearize(graph, SinkId(0)) === linearizationAtSink0)
    val linearizationAtSink1 = Seq(
      SourceId(1),
      SourceId(2),
      NodeId(1),
      NodeId(2),
      NodeId(4))
    assert(AnalysisUtils.linearize(graph, SinkId(1)) === linearizationAtSink1)
    val linearizationAtSink2 = Seq(
      NodeId(0),
      SourceId(3),
      NodeId(3),
      SourceId(1),
      SourceId(2),
      NodeId(1),
      NodeId(2),
      NodeId(4),
      NodeId(5),
      NodeId(8),
      NodeId(9),
      NodeId(6),
      NodeId(10),
      NodeId(13))
    assert(AnalysisUtils.linearize(graph, SinkId(2)) === linearizationAtSink2)
    val linearizationAtSink4 = Seq(
      NodeId(0),
      SourceId(3),
      NodeId(3),
      SourceId(1),
      SourceId(2),
      NodeId(1),
      NodeId(2),
      NodeId(4),
      NodeId(5),
      NodeId(8),
      NodeId(9),
      NodeId(6),
      NodeId(10),
      NodeId(14),
      NodeId(11),
      NodeId(12),
      NodeId(15),
      NodeId(7),
      NodeId(16),
      NodeId(17),
      NodeId(18))
    assert(AnalysisUtils.linearize(graph, SinkId(4)) === linearizationAtSink4)

    // Test deterministic, complete, full linearization
    val linearization = Seq(
      SourceId(2),
      SinkId(0),
      SourceId(1),
      NodeId(1),
      NodeId(2),
      NodeId(4),
      SinkId(1),
      NodeId(0),
      SourceId(3),
      NodeId(3),
      NodeId(5),
      NodeId(8),
      NodeId(9),
      NodeId(6),
      NodeId(10),
      NodeId(13),
      SinkId(2),
      NodeId(14),
      NodeId(11),
      NodeId(12),
      NodeId(15),
      NodeId(7),
      NodeId(16),
      NodeId(17),
      NodeId(18),
      SinkId(4)
    )

    assert(linearization === AnalysisUtils.linearize(graph))
  }
}
