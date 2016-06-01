package workflow.graph

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import pipelines.{LocalSparkContext, Logging}

class OperatorSuite extends FunSuite with LocalSparkContext with Logging {
  test("DatumOperator") {
    val datum = 4
    val op = new DatumOperator(datum)

    assert(datum === op.execute(Seq()).get)
  }

  test("DatasetOperator") {
    sc = new SparkContext("local", "test")

    val dataset = sc.parallelize(Seq(4, 5, 6))
    val op = new DatasetOperator(dataset)

    assert(dataset === op.execute(Seq()).get)
  }

  test("TransformerOperator single datums") {
    sc = new SparkContext("local", "test")

    val globalInt = new AtomicInteger(0)
    val inputs = Seq(2, 7, 3)
    val inputDatumOutputs = inputs.map(i => new DatumExpression(i))

    val transformer = new TransformerOperator {
      override private[graph] def singleTransform(dataDependencies: Seq[DatumExpression]): Any = {
        val sum = dataDependencies.map(_.get.asInstanceOf[Int]).sum
        globalInt.addAndGet(sum)
        sum
      }
      override private[graph] def batchTransform(dataDependencies: Seq[DatasetExpression]): RDD[_] = ???
    }

    // Test laziness of execution
    val out = transformer.execute(inputDatumOutputs).asInstanceOf[DatumExpression]
    assert(globalInt.get() === 0, "Transformer execution supposed to be lazy")

    assert(out.get === inputs.sum)
    assert(globalInt.get() === inputs.sum)

    // Test memoization of execution
    assert(out.get === inputs.sum)
    assert(globalInt.get() === inputs.sum)

    // Test laziness & memoization with other data
    val in2 = 13
    val out2 = transformer.execute(Seq(new DatumExpression(in2))).asInstanceOf[DatumExpression]
    assert(globalInt.get() === inputs.sum, "Transformer execution supposed to be lazy")

    assert(out2.get === in2)
    assert(globalInt.get() === (inputs.sum + in2))
    assert(out2.get === in2)
    assert(globalInt.get() === (inputs.sum + in2))
  }

  test("TransformerOperator batch datasets") {
    sc = new SparkContext("local", "test")

    val globalInt = new AtomicInteger(0)
    val dataset1 = sc.parallelize(Seq(2, 7, 3))
    val dataset2 = sc.parallelize(Seq(1, -4, 17))
    val dataset3 = sc.parallelize(Seq(1, 2))

    val transformer = new TransformerOperator {
      override private[graph] def singleTransform(dataDependencies: Seq[DatumExpression]): Any = ???
      override private[graph] def batchTransform(dataDependencies: Seq[DatasetExpression]): RDD[_] = {
        val rdds = dataDependencies.map(_.get.asInstanceOf[RDD[Int]])
        globalInt.addAndGet(rdds.map(_.sum.toInt).sum)
        rdds.head
      }
    }

    // Test laziness of execution
    val out = transformer.execute(Seq(new DatasetExpression(dataset1), new DatasetExpression(dataset2)))
      .asInstanceOf[DatasetExpression]
    assert(globalInt.get() === 0, "Transformer execution supposed to be lazy")

    assert(out.get === dataset1)
    assert(globalInt.get() === (dataset1.sum + dataset2.sum))

    // Test memoization of execution
    assert(out.get === dataset1)
    assert(globalInt.get() === (dataset1.sum + dataset2.sum))

    // Test laziness & memoization with other data
    val out2 = transformer.execute(Seq(new DatasetExpression(dataset3)))
      .asInstanceOf[DatasetExpression]
    assert(globalInt.get() === (dataset1.sum + dataset2.sum), "Transformer execution supposed to be lazy")
    assert(out2.get === dataset3)
    assert(globalInt.get() === (dataset1.sum + dataset2.sum + dataset3.sum))
    assert(out2.get === dataset3)
    assert(globalInt.get() === (dataset1.sum + dataset2.sum + dataset3.sum()))
  }

  test("TransformerOperator test invalid inputs") {
    sc = new SparkContext("local", "test")
    val datum = 4
    val dataset = sc.parallelize(Seq(datum))

    val transformer = new TransformerOperator {
      override private[graph] def singleTransform(dataDependencies: Seq[DatumExpression]): Any = datum
      override private[graph] def batchTransform(dataDependencies: Seq[DatasetExpression]): RDD[_] = dataset
    }

    // Expects exception to be returned when deps are not (all DatasetOutput or all DatumOutput)
    intercept[IllegalArgumentException] {
      transformer.execute(Seq(new DatasetExpression(dataset), new DatumExpression(datum)))
    }

    // Expects exception to be returned when deps are empty
    intercept[IllegalArgumentException] {
      transformer.execute(Seq())
    }
  }

  test("EstimatorOperator") {
    sc = new SparkContext("local", "test")

    val globalInt = new AtomicInteger(0)
    val dataset1 = sc.parallelize(Seq(2, 7, 3))
    val dataset2 = sc.parallelize(Seq(1, -4, 17))
    val dataset3 = sc.parallelize(Seq(1, 2))

    val dummyTransformer = new TransformerOperator {
      override private[graph] def singleTransform(dataDependencies: Seq[DatumExpression]): Any = ???
      override private[graph] def batchTransform(dataDependencies: Seq[DatasetExpression]): RDD[_] = ???
    }

    val estimator = new EstimatorOperator {
      override private[graph] def fitRDDs(inputs: Seq[DatasetExpression]): TransformerOperator = {
        val rdds = inputs.map(_.get.asInstanceOf[RDD[Int]])
        globalInt.addAndGet(rdds.map(_.sum.toInt).sum)
        dummyTransformer
      }
    }

    // Test laziness of execution
    val out = estimator.execute(Seq(new DatasetExpression(dataset1), new DatasetExpression(dataset2)))
      .asInstanceOf[TransformerExpression]
    assert(globalInt.get() === 0, "Estimator execution supposed to be lazy")

    assert(out.get === dummyTransformer)
    assert(globalInt.get() === (dataset1.sum + dataset2.sum))

    // Test memoization of execution
    assert(out.get === dummyTransformer)
    assert(globalInt.get() === (dataset1.sum + dataset2.sum))

    // Test laziness & memoization with other data
    val out2 = estimator.execute(Seq(new DatasetExpression(dataset3)))
      .asInstanceOf[TransformerExpression]
    assert(globalInt.get() === (dataset1.sum + dataset2.sum), "Transformer execution supposed to be lazy")
    assert(out2.get === dummyTransformer)
    assert(globalInt.get() === (dataset1.sum + dataset2.sum + dataset3.sum))
    assert(out2.get === dummyTransformer)
    assert(globalInt.get() === (dataset1.sum + dataset2.sum + dataset3.sum()))
  }

  test("DelegatingOperator single datums") {
    sc = new SparkContext("local", "test")

    val globalInt = new AtomicInteger(0)
    val inputs = Seq(2, 7, 3)
    val inputDatumOutputs: Seq[Expression] = inputs.map(i => new DatumExpression(i))

    val op = new DelegatingOperator
    val transformer = new TransformerExpression(new TransformerOperator {
      override private[graph] def singleTransform(dataDependencies: Seq[DatumExpression]): Any = {
        val sum = dataDependencies.map(_.get.asInstanceOf[Int]).sum
        globalInt.addAndGet(sum)
        sum
      }
      override private[graph] def batchTransform(dataDependencies: Seq[DatasetExpression]): RDD[_] = ???
    })

    // Test laziness of execution
    val out = op.execute(Seq(transformer) ++ inputDatumOutputs).asInstanceOf[DatumExpression]
    assert(globalInt.get() === 0, "Transformer execution supposed to be lazy")

    assert(out.get === inputs.sum)
    assert(globalInt.get() === inputs.sum)

    // Test memoization of execution
    assert(out.get === inputs.sum)
    assert(globalInt.get() === inputs.sum)

    // Test laziness & memoization with other data
    val in2 = 13
    val out2 = op.execute(Seq(transformer, new DatumExpression(in2))).asInstanceOf[DatumExpression]
    assert(globalInt.get() === inputs.sum, "Transformer execution supposed to be lazy")

    assert(out2.get === in2)
    assert(globalInt.get() === (inputs.sum + in2))
    assert(out2.get === in2)
    assert(globalInt.get() === (inputs.sum + in2))
  }

  test("DelegatingOperator batch datasets") {
    sc = new SparkContext("local", "test")

    val globalInt = new AtomicInteger(0)
    val dataset1 = sc.parallelize(Seq(2, 7, 3))
    val dataset2 = sc.parallelize(Seq(1, -4, 17))
    val dataset3 = sc.parallelize(Seq(1, 2))

    val op = new DelegatingOperator
    val transformer = new TransformerExpression(new TransformerOperator {
      override private[graph] def singleTransform(dataDependencies: Seq[DatumExpression]): Any = ???
      override private[graph] def batchTransform(dataDependencies: Seq[DatasetExpression]): RDD[_] = {
        val rdds = dataDependencies.map(_.get.asInstanceOf[RDD[Int]])
        globalInt.addAndGet(rdds.map(_.sum.toInt).sum)
        rdds.head
      }
    })

    // Test laziness of execution
    val out = op.execute(Seq(transformer, new DatasetExpression(dataset1), new DatasetExpression(dataset2)))
      .asInstanceOf[DatasetExpression]
    assert(globalInt.get() === 0, "Transformer execution supposed to be lazy")

    assert(out.get === dataset1)
    assert(globalInt.get() === (dataset1.sum + dataset2.sum))

    // Test memoization of execution
    assert(out.get === dataset1)
    assert(globalInt.get() === (dataset1.sum + dataset2.sum))

    // Test laziness & memoization with other data
    val out2 = op.execute(Seq(transformer, new DatasetExpression(dataset3)))
      .asInstanceOf[DatasetExpression]
    assert(globalInt.get() === (dataset1.sum + dataset2.sum), "Transformer execution supposed to be lazy")
    assert(out2.get === dataset3)
    assert(globalInt.get() === (dataset1.sum + dataset2.sum + dataset3.sum))
    assert(out2.get === dataset3)
    assert(globalInt.get() === (dataset1.sum + dataset2.sum + dataset3.sum()))
  }

  test("DelegatingOperator test invalid inputs") {
    sc = new SparkContext("local", "test")
    val datum = 4
    val dataset = sc.parallelize(Seq(datum))

    val op = new DelegatingOperator
    val transformer = new TransformerExpression(new TransformerOperator {
      override private[graph] def singleTransform(dataDependencies: Seq[DatumExpression]): Any = datum
      override private[graph] def batchTransform(dataDependencies: Seq[DatasetExpression]): RDD[_] = dataset
    })

    // Expects exception to be returned when deps are not (all DatasetOutput or all DatumOutput)
    intercept[IllegalArgumentException] {
      op.execute(Seq(transformer, new DatasetExpression(dataset), new DatumExpression(datum)))
    }

    // Expects exception to be returned when deps are empty
    intercept[IllegalArgumentException] {
      op.execute(Seq())
    }

    // Expects exception to be returned when only passed a transformer
    intercept[IllegalArgumentException] {
      op.execute(Seq(transformer))
    }
  }

}
