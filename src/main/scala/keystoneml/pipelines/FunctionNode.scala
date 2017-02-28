package keystoneml.pipelines

abstract class FunctionNode[A,B] extends (A => B) with Serializable