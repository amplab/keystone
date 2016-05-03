package workflow.graph

/**
 * Represents a pipeline transformation rule: A transformation from one pipeline
 * to a differently-executed but functionally equivalent pipeline
 */
abstract class Rule {
  /** Name for this rule, automatically inferred based on class name. */
  val ruleName: String = {
    val className = getClass.getName
    if (className endsWith "$") className.dropRight(1) else className
  }

  def apply(plan: Graph): Graph
}
