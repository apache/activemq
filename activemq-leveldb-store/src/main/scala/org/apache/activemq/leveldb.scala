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
package org.apache.activemq

import java.nio.ByteBuffer
import org.fusesource.hawtbuf.Buffer
import org.xerial.snappy.{Snappy => Xerial}
import org.iq80.snappy.{Snappy => Iq80}

/**
 * <p>
 * A Snappy abstraction which attempts uses the iq80 implementation and falls back
 * to the xerial Snappy implementation it cannot be loaded.  You can change the
 * load order by setting the 'leveldb.snappy' system property.  Example:
 *
 * <code>
 * -Dleveldb.snappy=xerial,iq80
 * </code>
 *
 * The system property can also be configured with the name of a class which
 * implements the Snappy.SPI interface.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
package object leveldb  {

  final val Snappy = {
    var attempt:SnappyTrait = null
    System.getProperty("leveldb.snappy", "iq80,xerial").split(",").foreach { x =>
      if( attempt==null ) {
        try {
            var name = x.trim();
            name = name.toLowerCase match {
              case "xerial" => "org.apache.activemq.leveldb.XerialSnappy"
              case "iq80" => "org.apache.activemq.leveldb.IQ80Snappy"
              case _ => name
            }
            attempt = Thread.currentThread().getContextClassLoader().loadClass(name).newInstance().asInstanceOf[SnappyTrait];
            attempt.compress("test")
        } catch {
          case x:Throwable =>
            attempt = null
        }
      }
    }
    attempt
  }


  trait SnappyTrait {
    
    def uncompressed_length(input: Buffer):Int
    def uncompress(input: Buffer, output:Buffer): Int
    
    def max_compressed_length(length: Int): Int
    def compress(input: Buffer, output: Buffer): Int

    def compress(input: Buffer):Buffer = {
      val compressed = new Buffer(max_compressed_length(input.length))
      compressed.length = compress(input, compressed)
      compressed
    }
    
    def compress(text: String): Buffer = {
      val uncompressed = new Buffer(text.getBytes("UTF-8"))
      val compressed = new Buffer(max_compressed_length(uncompressed.length))
      compressed.length = compress(uncompressed, compressed)
      return compressed
    }
    
    def uncompress(input: Buffer):Buffer = {
      val uncompressed = new Buffer(uncompressed_length(input))
      uncompressed.length = uncompress(input, uncompressed)
      uncompressed
    }

    def uncompress(compressed: ByteBuffer, uncompressed: ByteBuffer): Int = {
      val input = if (compressed.hasArray) {
        new Buffer(compressed.array, compressed.arrayOffset + compressed.position, compressed.remaining)
      } else {
        val t = new Buffer(compressed.remaining)
        compressed.mark
        compressed.get(t.data)
        compressed.reset
        t
      }

      val output = if (uncompressed.hasArray) {
        new Buffer(uncompressed.array, uncompressed.arrayOffset + uncompressed.position, uncompressed.capacity()-uncompressed.position)
      } else {
        new Buffer(uncompressed_length(input))
      }

      output.length = uncompress(input, output)

      if (uncompressed.hasArray) {
        uncompressed.limit(uncompressed.position + output.length)
      } else {
        val p = uncompressed.position
        uncompressed.limit(uncompressed.capacity)
        uncompressed.put(output.data, output.offset, output.length)
        uncompressed.flip.position(p)
      }
      return output.length
    }
  }
}
package leveldb {
  class XerialSnappy extends SnappyTrait {
    override def uncompress(compressed: ByteBuffer, uncompressed: ByteBuffer) = Xerial.uncompress(compressed, uncompressed)
    def uncompressed_length(input: Buffer) = Xerial.uncompressedLength(input.data, input.offset, input.length)
    def uncompress(input: Buffer, output: Buffer) = Xerial.uncompress(input.data, input.offset, input.length, output.data, output.offset)
    def max_compressed_length(length: Int) = Xerial.maxCompressedLength(length)
    def compress(input: Buffer, output: Buffer) = Xerial.compress(input.data, input.offset, input.length, output.data, output.offset)
    override def compress(text: String) = new Buffer(Xerial.compress(text))
  }

  class IQ80Snappy extends SnappyTrait {
    def uncompressed_length(input: Buffer) = Iq80.getUncompressedLength(input.data, input.offset)
    def uncompress(input: Buffer, output: Buffer): Int = Iq80.uncompress(input.data, input.offset, input.length, output.data, output.offset)
    def compress(input: Buffer, output: Buffer): Int = Iq80.compress(input.data, input.offset, input.length, output.data, output.offset)
    def max_compressed_length(length: Int) = Iq80.maxCompressedLength(length)
  }
}
