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
package org.apache.activemq.leveldb.replicated

import org.fusesource.hawtbuf.{Buffer, AsciiBuffer}
import org.fusesource.hawtdispatch.transport.AbstractProtocolCodec
import org.fusesource.hawtdispatch.transport.AbstractProtocolCodec.Action
import java.nio.{MappedByteBuffer, ByteBuffer}
import org.fusesource.hawtdispatch.Task
import java.io.{OutputStream, File}
import org.fusesource.hawtdispatch.transport.ProtocolCodec.BufferState
import java.util

class ReplicationFrame(val action:AsciiBuffer, _body:Buffer) {
  def body = _body
}
class FileTransferFrame(val file:File, val offset:Long, var length:Long)

class ReplicationProtocolCodec extends AbstractProtocolCodec {
  import ReplicationSupport._
  val transfers  = new util.LinkedList[MappedByteBuffer]();

  def encode(value: Any) {
    value match {
      case value:ReplicationFrame =>
        value.action.writeTo(nextWriteBuffer.asInstanceOf[OutputStream])
        nextWriteBuffer.write('\n');
        if( value.body!=null ) {
          value.body.writeTo(nextWriteBuffer.asInstanceOf[OutputStream])
        }
        nextWriteBuffer.write(0);
      case value:FileTransferFrame =>
        if( value.length > 0 ) {
          val buffer = map(value.file, value.offset, value.length, true)
          writeDirect(buffer);
          if( buffer.hasRemaining ) {
            transfers.addLast(buffer)
          } else {
            unmap(buffer)
          }
        }
      case value:Buffer =>
        value.writeTo(nextWriteBuffer.asInstanceOf[OutputStream])
    }
  }


  override def flush(): BufferState = {
    val rc = super.flush()
    while( !transfers.isEmpty && !transfers.peekFirst().hasRemaining) {
      unmap(transfers.removeFirst())
    }
    rc
  }

  def initialDecodeAction() = readHeader

  val readHeader = new Action() {
    def apply = {
      val action_line:Buffer = readUntil('\n'.toByte, 80)
      if( action_line!=null ) {
        action_line.moveTail(-1);
        nextDecodeAction = readReplicationFrame(action_line.ascii())
        nextDecodeAction.apply()
      } else {
        null
      }
    }
  }

  def readReplicationFrame(action:AsciiBuffer):Action = new Action() {
    def apply = {
      val data:Buffer = readUntil(0.toByte, 1024*64)
      if( data!=null ) {
        data.moveTail(-1);
        nextDecodeAction = readHeader
        new ReplicationFrame(action, data)
      } else {
        null
      }
    }
  }

  def readData(data_target:ByteBuffer, cb:Task) = {
    nextDecodeAction = new Action() {
      def apply = {
        if( readDirect(data_target) ) {
          nextDecodeAction = readHeader
          cb.run()
        }
        null
      }
    }
  }
}
