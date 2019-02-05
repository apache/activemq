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

import org.fusesource.hawtbuf.{Buffer, ByteArrayOutputStream}
import java.io.InputStream
import java.nio.charset.StandardCharsets

import javax.json.JsonObject
import javax.json.bind.{Jsonb, JsonbBuilder}
import javax.xml.bind.annotation.XmlRootElement

/**
 *
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object JsonCodec {

  final val mapper: Jsonb = JsonbBuilder.create()

  def decode[T](buffer: Buffer, clazz: Class[T]): T = {
    decode(buffer.in, clazz)
  }

  def decode[T](is: InputStream, clazz : Class[T]): T = {
    var original: ClassLoader = Thread.currentThread.getContextClassLoader
    Thread.currentThread.setContextClassLoader(this.getClass.getClassLoader)
    try {
      val wrapper = JsonCodec.mapper.fromJson(is, classOf[JsonObject])
      val name = clazz.getAnnotation(classOf[XmlRootElement]).name()
      val obj = wrapper.get(name)
      JsonCodec.mapper.fromJson(obj.asJsonObject().toString, clazz)
    }
    finally {
      Thread.currentThread.setContextClassLoader(original)
    }
  }


  def encode(value: AnyRef): Buffer = {
    var baos = new ByteArrayOutputStream
    baos.write("{\"".getBytes(StandardCharsets.UTF_8))
    baos.write(value.getClass.getAnnotation(classOf[XmlRootElement]).name().getBytes(StandardCharsets.UTF_8))
    baos.write("\":".getBytes(StandardCharsets.UTF_8))
    mapper.toJson(value, baos)
    baos.write("}".getBytes(StandardCharsets.UTF_8))
    baos.toBuffer
  }

}