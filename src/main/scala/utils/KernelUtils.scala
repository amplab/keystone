package utils

import nodes.stats._
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import breeze.linalg._
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

object KernelUtils extends Serializable {

  /**
   * Converts a matrix to an array of row arrays.
   * @param mat Input matrix.
   * @return Array of rows.
   */
  def matrixToRowArray[T: ClassTag](mat: DenseMatrix[T]): Array[DenseVector[T]] = {
    val matT = mat.t
    // The explicit copy of the vector is necessary because otherwise Breeze slices
    // lazily, leading to inflated serialization size (A serious issue w/ spark)
    (0 until mat.rows).toArray.map(x => DenseVector(matT(::, x).toArray))
  }

  /**
   * Converts a sequence of DenseVector to a matrix where each vector is a row.
   *
   * @param in Sequence of of DenseVectors (rows)
   * @return A row matrix.
   */
  def rowsToMatrix[T: ClassTag](in: TraversableOnce[DenseVector[T]]): DenseMatrix[T] = {
    rowsToMatrix(in.toArray)
  }

  /** 
   *  Converts a RDD of DenseVector to a RDD of DenseMatrix such that each partition
   *  of  DenseVectors are stacked into a rows to make  DenseMatrix
   */

  def rowsToMatrix[T: ClassTag] (in: RDD[DenseVector[T]]) : RDD[DenseMatrix[T]]= {
    in.mapPartitions { part =>
      Iterator.single(rowsToMatrix(part))
    }
  }

  /**
   * Converts a  RDD of DenseMatrix where each matrix is in one partition into 
   * an RDD of DenseVectors separating the matrix row wise.  Undefined behavior
   * if used on RDD of  DenseMatrix not generated  from above function
   */

  def matrixToRows [T: ClassTag] (in: RDD[DenseMatrix[T]]) : RDD[DenseVector[T]]= {
    in.mapPartitions { part =>
      val matrix = part.next()
      val nRows = matrix.rows
      val outArr = new Array[DenseVector[T]](nRows)
      var i = 0
      while (i < nRows)  {
        outArr(i) = matrix(::, i).toDenseVector
        i += 1
      }
      outArr.iterator
    }
  }
  /**
   * Converts an array of DenseVector to a matrix where each vector is a row.
   *
   * @param inArr Array of DenseVectors (rows)
   * @return A row matrix.
   */
  def rowsToMatrix[T: ClassTag](inArr: Array[DenseVector[T]]): DenseMatrix[T] = {
    val nRows = inArr.length
    val nCols = inArr(0).length
    val outArr = new Array[T](nRows * nCols)
    var i = 0
    while (i < nRows) {
      var j = 0
      val row = inArr(i)
      while (j < nCols) {
        outArr(i + nRows * j) = row(j)
        j = j + 1
      }
      i = i + 1
    }
    val outMat = new DenseMatrix[T](nRows, nCols, outArr)
    outMat
  }

  /**
   * Computes the squared Frobenius norm of an RDD containing partitions of matrix
   *
   * @param mat RDD of DenseMatrix
   * @return the squared Frobenius norm
   */
  def normFrobeniusSquared(mat: RDD[DenseMatrix[Double]]): Double = {
    mat.map(part=>math.pow(norm(part.toDenseVector), 2) ).reduce(_+_)
  }

  /**
   * Computes the squared Frobenius norm of underlying matrix
   *
   * @param Seq of RDD of DenseVector
   * @return the squared Frobenius norm
   */
  def distributedNormFrobeniusSquared(mat: Seq[RDD[DenseVector[Double]]]): Double = {
    mat.map(part => part.map(x => math.pow(norm(x), 2)).reduce(_ + _)).reduceLeft(_ + _)
  }

  /**
   * Computes the squared Frobenius norm of a Seq containing partitions of matrix
   *
   * @param mat Seq of DenseMatrix
   * @return the squared Frobenius norm
   */
  def normFrobeniusSquared(mat: Seq[DenseMatrix[Double]]): Double = {
    mat.map(part=>math.pow(norm(part.toDenseVector), 2)).reduce(_+_)
  }

  /**
   * Subtract the column-wise mean from each row of the feature matrix
   *
   * @param featuresPart: the feature matrix in dual format
   * @param featureMean: dx1 column-wise mean
   * @return the zero-meaned matrix
   */
  def zeroMeanFeatures(
    featuresPart: Seq[RDD[DenseMatrix[Double]]],
    featureMean:RDD[DenseVector[Double]]) = {
    featuresPart.map { blockFeat =>
      blockFeat.zip(featureMean).map { x =>
        x._1(::, *) - x._2
      }
    }
  }


  /**
   * Subtract the column-wise mean from each row of the label matrix
   *
   * @param labelsPart: the label matrix in dual format
   * @param labelMean: kx1 column-wise mean
   * @return the zero-meaned matrix
   */
  def zeroMeanLabels(
    labelsPart: Seq[RDD[DenseVector[Double]]],
    labelMean:RDD[Double]) = {
    labelsPart.map { blockLabels =>
      blockLabels.zip(labelMean).map { x =>
        x._1 - x._2
      }
    }
  }

  def blockDiag(firstPart: DenseMatrix[Double], secondPart: DenseMatrix[Double]) = {
    val out = DenseMatrix.zeros[Double](firstPart.rows + secondPart.rows, firstPart.cols + secondPart.cols)
    out(0 until firstPart.rows, 0 until firstPart.cols) := firstPart
    out(firstPart.rows until out.rows, firstPart.cols until out.cols) := secondPart
    out
  }

  def topK(ary: Array[Double], k: Int): Array[Int] = {
    ary.toSeq.zipWithIndex.sortBy(_._1).takeRight(k).map(_._2).toArray
  }

  def splitToBlocks(in: RDD[DenseVector[Double]], blockSize: Int, numFeatures: Int): Seq[RDD[DenseVector[Double]]] = {
    val numBlocks = math.ceil(numFeatures.toDouble / blockSize).toInt
    val inSplit = (0 until numBlocks).map { blockNum =>
      in.map { vec =>
        // Expliclity call toArray as breeze's slice is lazy
        val end = math.min(vec.length, (blockNum + 1) * blockSize)
        DenseVector(vec.slice(blockNum * blockSize, end).toArray)
      }
    }
    inSplit
  }

  def splitToBlocks(in: RDD[DenseVector[Double]], blockSizes: Seq[Int], numFeatures: Int): Seq[RDD[DenseVector[Double]]] = {
    // TODO: check if blockSizes adds up to numFeatures ?

    // Do a cumulative sum and all but last as begin and all but first as end
    val blockStarts = blockSizes.scanLeft(0)(_ + _).dropRight(1)
    val blockEnds = blockSizes.scanLeft(0)(_ + _).tail
    val blockStartEnds = blockStarts.zip(blockEnds)
    val inSplit = blockStartEnds.map { blockLimits =>
      in.map { vec =>
        // Expliclity call toArray as breeze's slice is lazy
        val end = math.min(vec.length, blockLimits._2)
        DenseVector(vec.slice(blockLimits._1, end).toArray)
      }
    }
    inSplit
  }

  def splitToBlocks(in: Array[Int], blockSize: Int, numFeatures: Int): Seq[Array[Int]] = {
    val numBlocks = math.ceil(numFeatures.toDouble / blockSize).toInt
    val inSplit = (0 until numBlocks).map { blockNum =>
      val end = math.min(in.length, (blockNum + 1) * blockSize)
      in.slice(blockNum * blockSize, end).toArray
    }
    inSplit
  }

  def shuffleArray[T](arr: Array[T]) = {
    // Shuffle each row in the same fashion
    val rnd = new java.util.Random(42)
    var i = arr.length - 1
    while (i > 0) {
      val index = rnd.nextInt(i + 1)
      // Simple swap
      val a = arr(index)
      arr(index) = arr(i)
      arr(i) = a
      i = i - 1
    }
    arr
  }

  //Returns a K^2 array ints
  def confusionMatrix(testLabels: Array[Int],
     predicted: Seq[DenseVector[Double]]): DenseMatrix[Double] = {

     val nClasses = predicted(0).length
     val map = DenseMatrix.zeros[Double](nClasses, nClasses)

     predicted.zip(testLabels).foreach { x =>
       // Get top-1 from predicted
       val topKPredicted: Int = KernelUtils.topK(x._1.toArray, 1)(0).toInt
       val actual: Int = x._2
       map(actual, topKPredicted) = map(actual, topKPredicted) + 1.0
     }
     map
  }

  def averagePolicy(preds: Array[DenseVector[Double]]): DenseVector[Double] = {
    preds.reduce(_ + _) :/ preds.size.toDouble
  }

  // * Let s(k) be the ordering of patch k.
  // * Let s(k)[i] be an integer, so that the worst item is ranked 1 and the best item is ranked n.
  // For i in images,
  //  For k in patches,
  //   score[i] += s(k)[i]
  def bordaPolicy(preds: Array[DenseVector[Double]]): DenseVector[Double] = {
    val ranks = preds.map { vec =>
      val sortedPreds = vec.toArray.zipWithIndex.sortBy(_._1).map(_._2)
      val rank = DenseVector(sortedPreds.zipWithIndex.sortBy(_._1).map(x => x._2.toDouble))
      rank
    }
    ranks.reduceLeft(_ + _)
  }

  def computeMultiViewError(
      names: RDD[String],
      predicted: RDD[DenseVector[Double]],
      actualLabels: RDD[Array[Int]],
      topK: Int): (Double, Double, Long) = {

    // associate a name with each predicted, actual
    val namedPreds = names.zip(predicted.zip(actualLabels.map(x => x(0))))
    // group by name to get all the predicted values for a name
    val groupedPreds = namedPreds.groupByKey(names.partitions.length).map { case (group, iter) =>
      val predActuals = iter.toArray // this is a array of tuples
      val predsForName = predActuals.map(_._1)
      assert(predActuals.map(_._2).distinct.size == 1)
      val actualForName: Int = predActuals.map(_._2).head
      
      (predsForName, actualForName)
    }.cache()

    // Averaging policy
    val finalPred = groupedPreds.map(x => (averagePolicy(x._1).toArray, x._2) )

    val err = finalPred.map { x =>
      // Get top-5 and top-1 from predicted
      val topKpredicted = KernelUtils.topK(x._1.toArray, topK)
      val top1predicted = KernelUtils.topK(x._1.toArray, 1)

      // Actual is Array with 1 entry
      val actual: Int = x._2
      val topKerr = if (!topKpredicted.contains(actual)) {
        1.0
      } else {
        0.0
      }
      val top1err = if (!top1predicted.contains(actual)) {
        1.0
      } else {
        0.0
      }
      (topKerr, top1err)
    }.reduce { case (x, y) => (x._1 + y._1,  x._2 + y._2) }

    val numTest = groupedPreds.count
    groupedPreds.unpersist()
  
    (err._1, err._2, numTest)
  }


  // topK
  def computeError(
      predicted: RDD[DenseVector[Double]],
      actualLabels: RDD[Array[Int]],
      topK: Int): (Double, Double) = {
    val err = predicted.zip(actualLabels).map { x =>
      // Get top-5 and top-1 from predicted
      val topKpredicted = KernelUtils.topK(x._1.toArray, topK)
      val top1predicted = KernelUtils.topK(x._1.toArray, 1)

      // Actual is Array with 1 entry
      val actual: Int = x._2(0)
      val topKerr = if (!topKpredicted.contains(actual)) {
        1.0
      } else {
        0.0
      }
      val top1err = if (!top1predicted.contains(actual)) {
        1.0
      } else {
        0.0
      }
      (topKerr, top1err)
    }.reduce { case (x, y) => (x._1 + y._1,  x._2 + y._2) }
    err
  }


  // Truncates the lineage of an RDD and returns a new RDD
  // that is in memory and has truncated lineage
  def truncateLineage[T: ClassTag](in: RDD[T], cache: Boolean): RDD[T] = {
    // What we are doing here is:
    // cache the input before checkpoint as it triggers a job
    if (cache) {
      in.cache()
    }
    in.checkpoint()
    // Run a count to trigger the checkpoint
    in.count

    // Now "in" has HDFS preferred locations which is bothersome
    // when we zip it next time. So do a identity map & get an RDD
    // that is in memory, but has no preferred locs
    val out = in.map(x => x).cache()
    // This stage will run as NODE_LOCAL ?
    out.count

    // Now out is in memory, we can get rid of "in" and then
    // return out
    if (cache) {
      in.unpersist(true)
    }
    out
  }

  /** Convert an MLlib vector to a Breeze dense vector */
  def mllibVectorToDenseBreeze(vector: org.apache.spark.mllib.linalg.Vector): DenseVector[Double] = {
    vector match {
      case dense: org.apache.spark.mllib.linalg.DenseVector => new DenseVector[Double](dense.values)
      case _ => new DenseVector[Double](vector.toArray)
    }
  }

  /** Convert an MLlib matrix to a Breeze dense matrix */
  def mllibMatrixToDenseBreeze(matrix: org.apache.spark.mllib.linalg.Matrix): DenseMatrix[Double] = {
    matrix match {
      case dense: org.apache.spark.mllib.linalg.DenseMatrix => {
        if (!dense.isTransposed) {
          new DenseMatrix[Double](dense.numRows, dense.numCols, dense.values)
        } else {
          val breezeMatrix = new DenseMatrix[Double](dense.numRows, dense.numCols, dense.values)
          breezeMatrix.t
        }
      }

      case _ => new DenseMatrix[Double](matrix.numRows, matrix.numCols, matrix.toArray)
    }
  }

  /** Convert a Breeze vector to an MLlib vector, maintaining underlying data structure (sparse vs dense) */
  def breezeVectorToMLlib(breezeVector: breeze.linalg.Vector[Double]): org.apache.spark.mllib.linalg.Vector = {
    breezeVector match {
      case v: DenseVector[Double] =>
        if (v.offset == 0 && v.stride == 1 && v.length == v.data.length) {
          new org.apache.spark.mllib.linalg.DenseVector(v.data)
        } else {
          new org.apache.spark.mllib.linalg.DenseVector(v.toArray)  // Can't use underlying array directly, so make a new one
        }
      case v: SparseVector[Double] =>
        if (v.index.length == v.used) {
          new org.apache.spark.mllib.linalg.SparseVector(v.length, v.index, v.data)
        } else {
          new org.apache.spark.mllib.linalg.SparseVector(v.length, v.index.slice(0, v.used), v.data.slice(0, v.used))
        }
      case v: breeze.linalg.Vector[_] =>
        sys.error("Unsupported Breeze vector type: " + v.getClass.getName)
    }
  }

  def computeWeights(trainingLabelsMat: RDD[DenseMatrix[Double]],
    mixtureWeight: Double,
    nTrain: Long): RDD[DenseMatrix[Double]] = {
    trainingLabelsMat.map { mat =>
      val numPosEx = mat.rows
      val firstLabel = mat(0, ::).t.toArray
      val classIdx = firstLabel.indexOf(firstLabel.max)
      val negWt = (1.0 - mixtureWeight) / nTrain.toDouble
      val posWt = negWt + (mixtureWeight / numPosEx.toDouble)
      val out = DenseMatrix.fill(mat.rows, mat.cols)(negWt)
      out(::, classIdx) := posWt
      out
    }
  }

  def nextPositivePowerOfTwo(i : Int) = 1 << (32 - Integer.numberOfLeadingZeros(i - 1))

  def isSorted[T](s: Seq[T])(implicit cmp: Ordering[T]): Boolean = {
    if (s.isEmpty) {
      true 
    } else {
      // a google search reveals that the most idiomatic way to do this scala
      // involves scalaz, links to a paper in the J. of Functional Programming,
      // and is nearly incomprehensible...
      //
      // I think I'll take a while loop and some mutable state. You can go
      // ahead and feel smug about your monads and semigroups while your code
      // is running 10x slower :)

      var i = 1
      while (i < s.size) {
        if (cmp.gt(s(i - 1), s(i)))
          return false
        i += 1
      }
      true
    }
  }
}

trait TimeUtils {
  def timeElasped(ns: Long) : Double = (System.nanoTime - ns).toDouble / 1e9
}
