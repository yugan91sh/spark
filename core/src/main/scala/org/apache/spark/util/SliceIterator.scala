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

private[spark] abstract class SliceIterator[U](from: Long, until: Long) extends Iterator[U] {

  private val start: Long = from max 0

  private val limit: Long =
    if (until < 0) -1 // unbounded
    else if (until <= start) 0 // empty
    else until - start // finite

  private var remaining = limit
  private var dropping = start

  final def hasNext: Boolean = { skip(); remaining != 0 && realHasNext() }

  final def next(): U = {
    skip()
    if (remaining > 0) {
      remaining -= 1
      realNext()
    } else if (unbounded) realNext()
    else Iterator.empty.next()
  }

  private def skip(): Unit =
    while (dropping > 0) {
      if (realHasNext()) {
        readNext()
        dropping -= 1
      } else {
        dropping = 0
      }
    }

  @inline private def unbounded = remaining < 0

  protected def realHasNext(): Boolean

  protected def realNext(): U

  protected def readNext(): U

}
