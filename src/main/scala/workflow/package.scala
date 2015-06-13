import workflow.Node

/**
 * Created by tomerk11 on 6/12/15.
 */
package object workflow {
  implicit class PipelineDSL[A, B](val pipeline: Pipeline[A, B]) extends AnyVal {
    def andThen[C](next: Pipeline[B, C]): Pipeline[A, C] = {
      val nodes = pipeline.nodes ++ next.nodes
      val dataDeps = pipeline.dataDeps ++ next.dataDeps.map(_.map {
        x => if (x == Pipeline.SOURCE) pipeline.sink else x + pipeline.nodes.size
      })
      val fitDeps = pipeline.fitDeps ++ next.fitDeps.map(_.map {
        x => if (x == Pipeline.SOURCE) pipeline.sink else x + pipeline.nodes.size
      })
      val sink = next.sink

      Pipeline(nodes, dataDeps, fitDeps, sink)
    }

  }
}

