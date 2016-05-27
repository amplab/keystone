package workflow.graph

// rough idea given incrementalism: do everything it can in the base executor (which may be shared w/ other things) w/o inserting sources.
// then create a "final executor" that is the base one w/ sources inserted, and optimized using the EquivalentNodeMerge optimizer.
// The final value execution happens on that "final executor"
// This two stage process allows "intuitive things" to happen a source being passed in is already processed elsewhere in the pipeline (e.g. making sure to reuse a cacher),
// while pipeline fitting results can be reused across multiple pipeline applies, as they all share the same base executor.
abstract class PipelineResult[T](
    private[graph] val executor: GraphExecutor,
    private[graph] val sink: SinkId
  ) {

  private lazy val result: T = executor.execute(sink).get.asInstanceOf[T]
  final def get(): T = result

}
