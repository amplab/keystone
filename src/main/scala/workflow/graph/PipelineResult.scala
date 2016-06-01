package workflow.graph

/**
 * A PipelineResult is a lazy wrapper around the result of applying a [[Pipeline]] to data.
 * Internally it contains the Pipeline's execution plan with data sources inserted,
 * and the sink that the Pipeline's output is expected to be produced by.
 *
 * @param executor The Pipeline's underlying execution plan,
 *                 with the Pipeline's sources inserted into the [[Graph]]
 * @param sink The Pipeline's sink
 * @tparam T The type of the result.
 */
abstract class PipelineResult[T] private[graph] (
    private[graph] val executor: GraphExecutor,
    private[graph] val sink: SinkId
  ) {

  private lazy val result: T = executor.execute(sink).get.asInstanceOf[T]
  final def get(): T = result

}
