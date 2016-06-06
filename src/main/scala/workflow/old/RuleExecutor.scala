package workflow

import pipelines.Logging

abstract class RuleExecutor extends Logging {

  /**
   * An execution strategy for rules that indicates the maximum number of executions. If the
   * execution reaches fix point (i.e. converge) before maxIterations, it will stop.
   */
  abstract class Strategy { def maxIterations: Int }

  /** A strategy that only runs once. */
  case object Once extends Strategy { val maxIterations = 1 }

  /** A strategy that runs until fix point or maxIterations times, whichever comes first. */
  case class FixedPoint(maxIterations: Int) extends Strategy

  /** A batch of rules. */
  protected case class Batch(name: String, strategy: Strategy, rules: Rule*)

  /** Defines a sequence of rule batches, to be overridden by the implementation. */
  protected val batches: Seq[Batch]

  /**
   * Executes the batches of rules defined by the subclass. The batches are executed serially
   * using the defined execution strategy. Within each batch, rules are also executed serially.
   */
  def execute[A, B](plan: Pipeline[A, B]): Pipeline[A, B] = {
    var curPlan = plan

    batches.foreach { batch =>
      val batchStartPlan = curPlan
      var iteration = 1
      var lastPlan = curPlan
      var continue = true

      // Run until fix point (or the max number of iterations as specified in the strategy.
      while (continue) {
        curPlan = batch.rules.foldLeft(curPlan) {
          case (prevPlan, rule) =>
            val result = rule(prevPlan)
            if (!(result == prevPlan)) {
              logTrace(
                s"""
                   |=== Applying Rule ${rule.ruleName} ===
                   |${prevPlan.toDOTString}
                   |${result.toDOTString}
                """.stripMargin)
            }

            result
        }
        iteration += 1
        if (iteration > batch.strategy.maxIterations) {
          // Only log if this is a rule that is supposed to run more than once.
          if (iteration != 2) {
            logInfo(s"Max iterations (${iteration - 1}) reached for batch ${batch.name}")
          }
          continue = false
        }

        if (curPlan planEquals lastPlan) {
          logTrace(
            s"Fixed point reached for batch ${batch.name} after ${iteration - 1} iterations.")
          continue = false
        }
        lastPlan = curPlan
      }

      if (!(batchStartPlan planEquals curPlan)) {
        logDebug(
          s"""
             |=== Result of Batch ${batch.name} ===
             |${plan.toDOTString}
             |${curPlan.toDOTString}
        """.stripMargin)
      } else {
        logTrace(s"Batch ${batch.name} has no effect.")
      }
    }

    curPlan
  }
}

abstract class Optimizer extends RuleExecutor