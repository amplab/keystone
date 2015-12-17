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

trait SparseGradient extends Serializable {

  def compute(
    data: SparseVector[Double],
    labels: DenseVector[Double],
    weights: DenseMatrix[Double],
    cumGradient: DenseMatrix[Double])
  : Double
}

class LeastSquaresSparseGradient(numClasses: Int) extends SparseGradient {

  def compute(
    data: SparseVector[Double],
    labels: DenseVector[Double],
    weights: DenseMatrix[Double],
    cumGradient: DenseMatrix[Double])
  : Double = {

    val loss = if (numClasses == 2) {
      // Data is  dx1

      // axb is 0
      var axb = 0.0

      var offset = 0
      while(offset < data.activeSize) {
        val index = data.indexAt(offset)
        val value = data.valueAt(offset)
        axb += weights.data(index) * value
        offset += 1
      }

      axb -= labels(0)

      offset = 0
      while(offset < data.activeSize) {
        val index = data.indexAt(offset)
        val value = data.valueAt(offset)
        val gradUpdate = (axb * value)
        cumGradient(index, 0) += gradUpdate
        offset += 1
      }

      val loss = 0.5 * axb * axb
      loss

    } else {
      // Least Squares Gradient is At.(Ax - b)
      val axb = weights.t * data
      axb -= labels

      var offset = 0
      while(offset < data.activeSize) {
        val index = data.indexAt(offset)
        val value = data.valueAt(offset)
        cumGradient(index, ::) += (axb.t * value)
        offset += 1
      }
      val loss = 0.5 * math.pow(norm(axb), 2)
      loss
    }
    loss
  }

}
