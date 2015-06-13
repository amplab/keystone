package workflow

/**
 * Represents a pipeline rewriting rule
 */
abstract class Rule {
  /** Name for this rule, automatically inferred based on class name. */
  val ruleName: String = {
    val className = getClass.getName
    if (className endsWith "$") className.dropRight(1) else className
  }

  def apply[A, B](plan: Pipeline[A, B]): Pipeline[A, B]
}
