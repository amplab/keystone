package internals

import org.apache.spark.rdd.RDD

sealed trait Output

class DatasetOutput(getRDD: => RDD[_]) extends Output {
  lazy val get: RDD[_] = getRDD
}

class DatumOutput(getDatum: => Any) extends Output {
  lazy val get: Any = getDatum
}

class TransformerOutput(getTransformer: => TransformerOperator) extends Output {
  lazy val get: TransformerOperator = getTransformer
}
