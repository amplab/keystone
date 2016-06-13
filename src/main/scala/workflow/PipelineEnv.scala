package workflow

/**
 * PipelineEnv is an environment shared by multiple [[Pipeline]]s, containing variables
 * such as the Prefix state table and the current Pipeline [[Optimizer]].
 */
class PipelineEnv {
  /**
   * This is the global execution state of Pipelines with this environment.
   * It is a mutable hashmap of logical prefix to the executed result at that prefix.
   * It is not currently thread-safe.
   */
  private[workflow] val state: scala.collection.mutable.Map[Prefix, Expression] = scala.collection.mutable.Map()

  /**
   * The internally stored optimizer used for all Pipeline execution. Accessible using getter and setter.
   */
  private var _optimizer: Optimizer = DefaultOptimizer

  /**
   * @return The current optimizer used during Pipeline execution.
   */
  def getOptimizer: Optimizer = _optimizer

  /**
   * Globally set a new optimizer to use during Pipeline execution.
   *
   * @param optimizer The new optimizer to use
   */
  def setOptimizer(optimizer: Optimizer): Unit = {
    _optimizer = optimizer
  }

  /**
   * Reset this PipelineEnv (clear state and set the Optimizer to the DefaultOptimizer)
   */
  private [workflow] def reset(): Unit = {
    state.clear()
    setOptimizer(DefaultOptimizer)
  }
}

object PipelineEnv {
  lazy val getOrCreate: PipelineEnv = new PipelineEnv
}
