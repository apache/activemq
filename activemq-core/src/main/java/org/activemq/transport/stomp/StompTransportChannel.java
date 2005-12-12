/**
 * 
 * Copyright 2005 Protique Ltd
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 * 
 **/

package org.activemq.transport.stomp;

import org.activemq.transport.tcp.TcpTransport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;

/**
 * A transport for using Stomp to talk to ActiveMQ
 * 
 * @version $Revision: 1.1 $
 */
public class StompTransportChannel extends TcpTransport {
    private static final Log log = LogFactory.getLog(StompTransportChannel.class);

    public StompTransportChannel() {
        super(new StompWireFormat());
    }

    public StompTransportChannel(URI remoteLocation) throws UnknownHostException, IOException {
        super(new StompWireFormat(), remoteLocation);
    }

    public StompTransportChannel(URI remoteLocation, URI localLocation) throws UnknownHostException, IOException {
        super(new StompWireFormat(), remoteLocation, localLocation);
    }

    public StompTransportChannel(Socket socket) throws IOException {
        super(new StompWireFormat(), socket);
    }

    /*
     * protected void readWireFormat() throws JMSException, IOException { // no
     * need to read wire format from wire }
     * 
     * protected void doConsumeCommand(Command packet) { if( packet ==
     * FlushCommand.PACKET ) { try { doAsyncSend(null); } catch (JMSException e) {
     * ExceptionListener listener = getExceptionListener(); if (listener !=
     * null) { listener.onException(e); } else { log.warn("No listener to report
     * error consuming packet: " + e, e); } } } else {
     * super.doConsumeCommand(packet); } }
     */

}
