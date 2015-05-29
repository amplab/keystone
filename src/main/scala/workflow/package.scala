import scala.reflect.ClassTag

/**
 * Created by tomerk11 on 5/28/15.
 */
package object workflow {
  implicit def nodeToPipeline[A, B : ClassTag](node: Node[A, B]): Pipeline[A, B] = {
    node match {
      case Pipeline(nodes) => Pipeline[A, B](nodes)
      case _ => Pipeline[A, B](node.rewrite)
    }
  }
}
