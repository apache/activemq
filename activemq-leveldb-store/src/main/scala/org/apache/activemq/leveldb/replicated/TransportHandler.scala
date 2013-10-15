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

import org.fusesource.hawtdispatch.transport.{TransportListener, DefaultTransportListener, Transport}
import java.util
import org.apache.activemq.leveldb.replicated.ReplicationSupport._
import org.fusesource.hawtdispatch._
import org.apache.activemq.leveldb.util.JsonCodec
import java.io.IOException
import org.fusesource.hawtbuf.AsciiBuffer

/**
 */
abstract class TransportHandler(val transport: Transport) extends TransportListener {

  var outbound = new util.LinkedList[AnyRef]()
  val codec = new ReplicationProtocolCodec

  transport.setProtocolCodec(codec)
  transport.setTransportListener(this)

  def start = {
    transport.start(NOOP)
  }

  def onTransportConnected = transport.resumeRead()
  def onTransportDisconnected() = {}
  def onRefill = drain
  def onTransportFailure(error: IOException) = transport.stop(NOOP)

  def drain:Unit = {
    while( !outbound.isEmpty ) {
      val value = outbound.peekFirst()
      if( transport.offer(value) ) {
        outbound.removeFirst()
      } else {
        return
      }
    }
  }

  def send(value:AnyRef):Unit = {
    transport.getDispatchQueue.assertExecuting()
    outbound.add(value)
    drain
  }

  def send(action:AsciiBuffer, body:AnyRef):Unit = send(ReplicationFrame(action, if(body==null) null else JsonCodec.encode(body)))
  def sendError(error:String) = send(ERROR_ACTION, error)
  def sendOk(body:AnyRef) = send(OK_ACTION, body)

}
