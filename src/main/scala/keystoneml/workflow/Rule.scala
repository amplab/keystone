package keystoneml.workflow

/**
 * Represents a DAG transformation rule: A transformation from one DAG
 * to a differently-executed but logically equivalent DAG.
 *
 * A rule must also produce execution state for
 * the new DAG, logically equivalent to the execution state
 * attached to the old DAG.
 */
abstract class Rule {
  /** Name for this rule, automatically inferred based on class name. */
  val ruleName: String = {
    val className = getClass.getName
    if (className endsWith "$") className.dropRight(1) else className
  }

  def apply(plan: Graph, prefixes: Map[NodeId, Prefix]): (Graph, Map[NodeId, Prefix])
}
