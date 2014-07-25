/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.leveldb.util

import com.fasterxml.jackson.databind.ObjectMapper
import org.fusesource.hawtbuf.{ByteArrayOutputStream, Buffer}
import java.io.InputStream

/**
 *
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object JsonCodec {

  final val mapper: ObjectMapper = new ObjectMapper

  def decode[T](buffer: Buffer, clazz: Class[T]): T = {
    val original = Thread.currentThread.getContextClassLoader
    Thread.currentThread.setContextClassLoader(this.getClass.getClassLoader)
    try {
      return mapper.readValue(buffer.in, clazz)
    } finally {
      Thread.currentThread.setContextClassLoader(original)
    }
  }

  def decode[T](is: InputStream, clazz : Class[T]): T = {
    var original: ClassLoader = Thread.currentThread.getContextClassLoader
    Thread.currentThread.setContextClassLoader(this.getClass.getClassLoader)
    try {
      return JsonCodec.mapper.readValue(is, clazz)
    }
    finally {
      Thread.currentThread.setContextClassLoader(original)
    }
  }


  def encode(value: AnyRef): Buffer = {
    var baos = new ByteArrayOutputStream
    mapper.writeValue(baos, value)
    return baos.toBuffer
  }

}