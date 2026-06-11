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

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.DataByteArrayInputStream;

public class StompCodec {

    private final static byte[] crlfcrlf = new byte[]{'\r','\n','\r','\n'};
    private final TcpTransport transport;
    private final StompWireFormat wireFormat;

    private final ByteArrayOutputStream currentCommand = new ByteArrayOutputStream();
    private boolean processedHeaders = false;
    private String action;
    private Map<String, String> headers;
    private int contentLength = -1;
    private int readLength = 0;
    private int previousByte = -1;
    private boolean awaitingCommandStart = true;

    public StompCodec(TcpTransport transport) {
        this.transport = Objects.requireNonNull(transport);
        this.wireFormat = (StompWireFormat) Objects.requireNonNull(transport.getWireFormat());
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

               // skip heart beat commands.
               if (awaitingCommandStart && b == '\n') {
                   continue;
               } else {
                   awaitingCommandStart = false;   // non-newline indicates next frame.
               }

               currentCommand.write(b);

               if (currentCommand.size() > wireFormat.getMaxFrameSize()) {
                   StompFrameError errorFrame = new StompFrameError(
                           new ProtocolException("The maximum frame size was exceeded while processing headers.", true));
                   errorFrame.setAction(this.action);
                   transport.doConsume(errorFrame);
                   return;
               }

               // end of headers section, parse action and header
               if (b == '\n' && (previousByte == '\n' || currentCommand.endsWith(crlfcrlf))) {
                   DataByteArrayInputStream data = new DataByteArrayInputStream(currentCommand.toByteArray());

                   try {
                       action = wireFormat.parseAction(data);
                       wireFormat.validateAction(action);
                       headers = wireFormat.parseHeaders(data);
                       
                       String contentLengthHeader = headers.get(Stomp.Headers.CONTENT_LENGTH);
                       if ((action.equals(Stomp.Commands.SEND) || action.equals(Stomp.Responses.MESSAGE)) && contentLengthHeader != null) {
                           contentLength = wireFormat.parseContentLength(contentLengthHeader);
                       } else {
                           contentLength = -1;
                       }
                   } catch (ProtocolException e) {
                       transport.doConsume(new StompFrameError(e));
                       return;
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
                       if (currentCommand.size() > wireFormat.getMaxDataLength()) {
                           StompFrameError errorFrame = new StompFrameError(new ProtocolException("The maximum data length was exceeded", true));
                           errorFrame.setAction(this.action);
                           transport.doConsume(errorFrame);
                           return;
                       }
                       if (wireFormat.incrementAndGetFrameSize() > wireFormat.getMaxFrameSize()) {
                           StompFrameError errorFrame = new StompFrameError(new ProtocolException("The maximum frame size was exceeded", true));
                           errorFrame.setAction(this.action);
                           transport.doConsume(errorFrame);
                           return;
                       }
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
        awaitingCommandStart = true;
        currentCommand.reset();
        contentLength = -1;
        wireFormat.resetFrame();
    }

    public static String detectVersion(Map<String, String> headers) throws ProtocolException {
        String accepts = headers.get(Stomp.Headers.Connect.ACCEPT_VERSION);

        if (accepts == null) {
            accepts = Stomp.DEFAULT_VERSION;
        }
        HashSet<String> acceptsVersions = new HashSet<String>(Arrays.asList(accepts.trim().split(Stomp.COMMA)));
        acceptsVersions.retainAll(Arrays.asList(Stomp.SUPPORTED_PROTOCOL_VERSIONS));
        if (acceptsVersions.isEmpty()) {
            throw new ProtocolException("Invalid Protocol version[" + accepts +"], supported versions are: " +
                    Arrays.toString(Stomp.SUPPORTED_PROTOCOL_VERSIONS), true);
        } else {
            return Collections.max(acceptsVersions);
        }
    }
}
