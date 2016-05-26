package workflow.graph

// rough idea given incrementalism: do everything it can in the base executor (which may be shared w/ other things) w/o inserting sources.
// then create a "final executor" that is the base one w/ sources inserted, and optimized using the EquivalentNodeMerge optimizer.
// The final value execution happens on that "final executor"
// This two stage process allows "intuitive things" to happen a source being passed in is already processed elsewhere in the pipeline (e.g. making sure to reuse a cacher),
// while pipeline fitting results can be reused across multiple pipeline applies, as they all share the same base executor.
abstract class PipelineResult[T](
    executor: GraphExecutor,
    sink: SinkId
  ) {
  private var _executor: GraphExecutor = executor
  private var _sink: SinkId = sink

  private[graph] def setExecutor(executor: GraphExecutor): Unit = {
    this._executor = executor
  }

  private[graph] def setSink(sink: SinkId): Unit = {
    this._sink = sink
  }

  private[graph] def getSink: SinkId = _sink

  private[graph] def getGraph: Graph = executor.graph

  private lazy val result: T = _executor.execute(getSink).get.asInstanceOf[T]
  final def get(): T = result

}
