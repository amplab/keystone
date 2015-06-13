package workflow

import scala.reflect.ClassTag

/**
 * Created by tomerk11 on 5/28/15.
 */
abstract class OldNode[A, B : ClassTag] extends Serializable {
  def rewrite: Seq[OldNode[_, _]]
  def canSafelyPrependExtraNodes: Boolean
}

object OldNode {
  /**
   * An implicit conversion to turn any [[OldNode]] into a [[OldPipeline]], to allow chaining, fitting, etc.
   */
  implicit def nodeToPipeline[A, B : ClassTag](node: OldNode[A, B]): OldPipeline[A, B] = {
    OldPipeline(node.rewrite)
  }
}