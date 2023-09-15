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

package org.apache.spark.util

import scala.collection.Iterator

private[spark] abstract class SliceNextIterator[U](from: Long, until: Long) extends Iterator[U] {

  // from -> start
  private val start: Long = from max 0

  // until -> limit
  private val limit: Long =
    if (until < 0) -1 // unbounded
    else if (until <= start) 0 // empty
    else until - start // finite
  // next iterator
  protected var finished = false
  // slice iterator
  private var remaining: Long = limit
  private var dropping: Long = start
  // next iterator
  private var gotNext = false
  private var nextValue: U = _
  private var closed = false

  override def hasNext: Boolean = {
    skip(); remaining != 0 && realHasNext
  }

  override def next(): U = {
    skip()
    if (remaining > 0) {
      remaining -= 1
      realNext()
    } else if (unbounded) realNext()
    else Iterator.empty.next()
  }

  def closeIfNeeded(): Unit = {
    if (!closed) {
      // Note: it's important that we set closed = true before calling close(), since setting it
      // afterwards would permit us to call close() multiple times if close() threw an exception.
      closed = true
      close()
    }
  }

  protected def unbounded: Boolean = remaining < 0

  protected def readNext(): Unit

  protected def getNext(): U

  protected def close(): Unit

  protected def realHasNext: Boolean = {
    if (!finished) {
      if (!gotNext) {
        nextValue = getNext()
        if (finished) {
          closeIfNeeded()
        }
        gotNext = true
      }
    }
    !finished
  }

  private def skip(): Unit =
    while (dropping > 0) {
      if (realHasNext) {
        readNext()
        dropping -= 1
      } else {
        dropping = 0
      }
    }

  private def realNext(): U = {
    if (!realHasNext) {
      throw new NoSuchElementException("End of stream")
    }
    gotNext = false
    nextValue
  }

}
