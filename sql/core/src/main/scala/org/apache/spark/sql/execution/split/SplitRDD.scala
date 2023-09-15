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

import org.apache.spark.{Dependency, NarrowDependency, Partition, TaskContext}
import org.apache.spark.rdd.RDD

private[spark] case class SplitRDDPartition[U](
    index: Int,
    readFrom: Long,
    readUntil: Long,
    @transient parentRdd: RDD[U],
    parentIndex: Int)
    extends Partition {
  var parentPartition: Partition = parentRdd.partitions(parentIndex)

  var prefLoc: Seq[String] =
    parentRdd.context.getPreferredLocs(parentRdd, parentIndex).map(_.host)

}

class SplitRDD[U: ClassTag](
    @transient var prev: RDD[U],
    rowCount: Long,
    partNum: Int,
    splitter: Option[PartitionSplitter[U]] = None)
    extends RDD[U](prev.context, Nil) { // Nil since we implement getDependencies

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(partition: Partition, context: TaskContext): Iterator[U] = {
    val splitPart = partition.asInstanceOf[SplitRDDPartition[U]]
    firstParent[U]
      .sliceIterator(splitPart.readFrom, splitPart.readUntil, splitPart.parentPartition, context)
  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   *
   * The partitions in this array must satisfy the following property:
   * `rdd.partitions.zipWithIndex.forall { case (partition, index) => partition.index == index }`
   */
  override protected def getPartitions: Array[Partition] = {
    val ps = splitter.getOrElse(new DefaultPartitionSplitter[U]())
    ps.split(rowCount, partNum, prev)
  }

  /**
   * Implemented by subclasses to return how this RDD depends on parent RDDs. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override protected def getDependencies: Seq[Dependency[_]] = {
    Seq(new NarrowDependency(prev) {
      def getParents(id: Int): Seq[Int] =
        Seq(partitions(id).asInstanceOf[SplitRDDPartition[U]].parentIndex)
    })
  }

  /**
   * Clears the dependencies of this RDD. This method must ensure that all references
   * to the original parent RDDs are removed to enable the parent RDDs to be garbage
   * collected. Subclasses of RDD may override this method for implementing their own cleaning
   * logic. See [[org.apache.spark.rdd.UnionRDD]] for an example.
   */
  override protected def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }

  /**
   * Optionally overridden by subclasses to specify placement preferences.
   */
  override protected def getPreferredLocations(partition: Partition): Seq[String] = {
    partition.asInstanceOf[SplitRDDPartition[U]].prefLoc
  }

}

private[spark] class DefaultPartitionSplitter[U] extends PartitionSplitter[U] {

  override def split(readCount: Long, partNum: Int, parent: RDD[U]): Array[Partition] = {
    parent.partitions.zipWithIndex.flatMap { case (_, pi) =>
      (0 until partNum).map { i =>
        val index = pi * partNum + i
        val readFrom = readCount * i
        val readUtil = if (i == (partNum - 1)) {
          -1L // unbounded
        } else {
          readFrom + readCount
        }
        SplitRDDPartition[U](index, readFrom, readUtil, parent, pi)
      }
    }
  }

}
