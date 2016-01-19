/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nodes.learning

import breeze.linalg._
import org.apache.spark.rdd.RDD
import workflow.Transformer

/**
 * Computes A * x + b i.e. a linear map of data using a trained model.
 *
 * @param x trained model
 * @param bOpt optional intercept to add
 */
case class SparseLinearMapper(
    x: DenseMatrix[Double],
    bOpt: Option[DenseVector[Double]] = None)
  extends Transformer[SparseVector[Double], DenseVector[Double]] {

  /**
   * Apply a linear model to an input.
   * @param in Input.
   * @return Output.
   */
  def apply(in: SparseVector[Double]): DenseVector[Double] = {
    val out = x.t * in
    val bout = bOpt.map { b =>
      out :+= b
    }.getOrElse(out)

    if (bout.length == 1) {
      val boutEx = (DenseVector.ones[Double](2) * -1.0)
      if (bout(0) > 0) {
        boutEx(1) = 1.0
      } else {
        boutEx(0) = 1.0
      }
      boutEx
    } else {
      out
    }
  }

  /**
   * Apply a linear model to a collection of inputs.
   *
   * @param in Collection of A's.
   * @return Collection of B's.
   */
  override def apply(in: RDD[SparseVector[Double]]): RDD[DenseVector[Double]] = {
    val modelBroadcast = in.context.broadcast(x)
    val bBroadcast = in.context.broadcast(bOpt)
    in.map(row => {
      val out = modelBroadcast.value.t * row
      val bout = bOpt.map { b =>
        out :+= b
      }.getOrElse(out)

      if (bout.length == 1) {
        val boutEx = (DenseVector.ones[Double](2) * -1.0)
        if (bout(0) > 0) {
          boutEx(1) = 1.0
        } else {
          boutEx(0) = 1.0
        }
        boutEx
      } else {
        out
      }
    })
  }
}
