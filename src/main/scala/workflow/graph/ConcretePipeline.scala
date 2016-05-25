package workflow.graph

/**
 * An implementation of [[Pipeline]] that explicitly specifies the Pipline internals as vals.
 *
 * @param executor The [[GraphExecutor]] underlying the Pipeline execution.
 * @param source The SourceId of the pipeline
 * @param sink The SinkId of the Pipeline
 * @tparam A The type of input to the Pipeline
 * @tparam B The type of output from the Pipeline
 */
private[graph] class ConcretePipeline[A, B](
  override private[graph] val executor: GraphExecutor,
  override private[graph] val source: SourceId,
  override private[graph] val sink: SinkId
) extends Pipeline[A, B]
