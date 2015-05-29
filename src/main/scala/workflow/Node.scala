package workflow

import scala.reflect.ClassTag

/**
 * Created by tomerk11 on 5/28/15.
 */
abstract class Node[A, B : ClassTag] extends Serializable {
  def rewrite: Seq[Node[_, _]]
  def canElevate: Boolean
}

object Node {
  implicit def nodeToPipeline[A, B : ClassTag](node: Node[A, B]): Pipeline[A, B] = {
    node match {
      case Pipeline(nodes) => Pipeline[A, B](nodes)
      case _ => Pipeline[A, B](node.rewrite)
    }
  }
}