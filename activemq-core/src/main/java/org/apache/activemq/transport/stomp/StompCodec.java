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
package org.apache.activemq.transport.stomp;

import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.DataByteArrayInputStream;

import java.io.ByteArrayInputStream;
import java.util.HashMap;

public class StompCodec {

    TcpTransport transport;

    ByteArrayOutputStream currentCommand = new ByteArrayOutputStream();
    boolean processedHeaders = false;
    String action;
    HashMap<String, String> headers;
    int contentLength = -1;
    int readLength = 0;
    int previousByte = -1;

    public StompCodec(TcpTransport transport) {
        this.transport = transport;
    }

    public void parse(ByteArrayInputStream input, int readSize) throws Exception {
       int i = 0;
       int b;
       while(i++ < readSize) {
           b = input.read();
           // skip repeating nulls
           if (!processedHeaders && previousByte == 0 && b == 0) {
               continue;
           }

           if (!processedHeaders) {
               currentCommand.write(b);
               // end of headers section, parse action and header
               if (previousByte == '\n' && b == '\n') {
                   if (transport.getWireFormat() instanceof StompWireFormat) {
                       DataByteArrayInputStream data = new DataByteArrayInputStream(currentCommand.toByteArray());
                       action = ((StompWireFormat)transport.getWireFormat()).parseAction(data);
                       headers = ((StompWireFormat)transport.getWireFormat()).parseHeaders(data);
                       String contentLengthHeader = headers.get(Stomp.Headers.CONTENT_LENGTH);
                       if (contentLengthHeader != null) {
                           contentLength = ((StompWireFormat)transport.getWireFormat()).parseContentLength(contentLengthHeader);
                       } else {
                           contentLength = -1;
                       }
                   }
                   processedHeaders = true;
                   currentCommand.reset();
               }
           } else {

               if (contentLength == -1) {
                   // end of command reached, unmarshal
                   if (b == 0) {
                       processCommand();
                   } else {
                       currentCommand.write(b);
                   }
               } else {
                   // read desired content length
                   if (readLength++ == contentLength) {
                       processCommand();
                       readLength = 0;
                   } else {
                       currentCommand.write(b);
                   }
               }
           }

           previousByte = b;
       }
    }

    protected void processCommand() throws Exception {
        StompFrame frame = new StompFrame(action, headers, currentCommand.toByteArray());
        transport.doConsume(frame);
        processedHeaders = false;
        currentCommand.reset();
        contentLength = -1;
    }
}
