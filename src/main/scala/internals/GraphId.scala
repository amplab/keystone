package internals

sealed trait GraphId { val id: Long }

case class SinkId(id: Long) extends GraphId

sealed trait NodeOrSourceId extends GraphId

case class NodeId(id: Long) extends NodeOrSourceId

case class SourceId(id: Long) extends NodeOrSourceId
