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

package org.apache.spark.sql.execution.split

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class SplitExec(rowCount: Long, partitionNum: Int, preferShuffle: Boolean, child: SparkPlan)
    extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): SplitExec =
    copy(child = newChild)

  /**
   * Produces the result of the query as an `RDD[InternalRow]`
   *
   * Overridden by concrete implementations of SparkPlan.
   */
  override protected def doExecute(): RDD[InternalRow] = {
    doSplit(child.execute())
  }

  private def doSplit[U: ClassTag](prev: RDD[U]): RDD[U] = {
    val parallelism = sparkContext.defaultParallelism
    val partNum = (sparkContext.defaultMinPartitions max
      (parallelism / partitionNum)) max (parallelism min partitionNum)
    if (prev.getNumPartitions < partNum) {
      if (preferShuffle) {
        // Currently not supported: supportsColumnar, because `ColumnarBatch` is not serializable.
        // But maybe we can fix this in the future.
        prev.coalesce(partNum, shuffle = true)
      } else {
        new SplitRDD[U](prev, rowCount, partNum)
      }
    } else {
      prev
    }
  }

}
