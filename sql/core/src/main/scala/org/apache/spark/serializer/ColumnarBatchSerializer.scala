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

package org.apache.spark.serializer

import java.io._
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Serializer for serializing `ColumnarBatch`s for use during normal shuffle.
 */
class ColumnarBatchSerializer(conf: SparkConf)
    extends org.apache.spark.serializer.Serializer
    with Logging
    with Serializable {

  /** Creates a new [[SerializerInstance]]. */
  override def newInstance(): SerializerInstance = {
    new ColumnarBatchSerializerInstance()
  }

}

private[spark] class ColumnarBatchSerializationStream(out: OutputStream)
    extends SerializationStream {

  private val dataOutput = new DataOutputStream(new BufferedOutputStream(out))

  /** Writes the object representing the value of a key-value pair. */
  override def writeValue[T: ClassTag](value: T): SerializationStream = {
    /* val batch = value.asInstanceOf[ColumnarBatch]


    dataOutput.writeInt(batch.numRows())
    dataOutput.writeInt(batch.numCols())

    for (i <- 0 until batch.numCols()) {
      val cv = batch.column(i)

    } */

    throw new UnsupportedOperationException()
  }


  override def writeKey[T: ClassTag](key: T): SerializationStream = {
    // The key is only needed on the map side when computing partition ids. It does not need to
    // be shuffled.
    assert(null == key || key.isInstanceOf[Int])
    this
  }

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    // This method is never called by shuffle code.
    throw new UnsupportedOperationException
  }

  override def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
    // This method is never called by shuffle code.
    throw new UnsupportedOperationException
  }

  override def flush(): Unit = {
    dataOutput.flush()
  }

  override def close(): Unit = {
    dataOutput.close()
  }

}

private[spark] class ColumnarBatchDeserializationStream(in: InputStream)
    extends DeserializationStream {

  private val dataInput = new DataInputStream(new BufferedInputStream(in))

  override def asKeyValueIterator: Iterator[(Int, ColumnarBatch)] = {
    // TODO
    throw new UnsupportedOperationException()
  }

  override def asIterator: Iterator[Any] = {
    // This method is never called by shuffle code.
    throw new UnsupportedOperationException
  }

  override def readKey[T]()(implicit classType: ClassTag[T]): T = {
    // We skipped serialization of the key in writeKey(), so just return a dummy value since
    // this is going to be discarded anyways.
    null.asInstanceOf[T]
  }

  override def readValue[T]()(implicit classType: ClassTag[T]): T = {
    // This method should never be called by shuffle code.
    throw new UnsupportedOperationException
  }

  override def readObject[T]()(implicit classType: ClassTag[T]): T = {
    // This method is never called by shuffle code.
    throw new UnsupportedOperationException
  }

  override def close(): Unit = {
    dataInput.close()
  }

}

private[spark] class ColumnarBatchSerializerInstance extends SerializerInstance {

  // This method is never called by shuffle code.
  override def serialize[T: ClassTag](t: T): ByteBuffer =
    throw new UnsupportedOperationException()

  // This method is never called by shuffle code.
  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException()

  // This method is never called by shuffle code.
  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException()

  override def serializeStream(s: OutputStream): SerializationStream = {
    new ColumnarBatchSerializationStream(s)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new ColumnarBatchDeserializationStream(s)
  }

}

private object ColumnarBatchSerializer {}

private[spark] class SerializedHeader {

}

private[spark] class ColumnHeader{

}
