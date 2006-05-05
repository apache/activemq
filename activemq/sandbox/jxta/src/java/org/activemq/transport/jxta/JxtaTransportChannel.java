/** 
 * 
 * Copyright 2004 Protique Ltd
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
package org.activemq.transport.jxta;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.activemq.io.WireFormat;
import org.activemq.transport.tcp.TcpTransportChannel;
import org.p2psockets.P2PInetAddress;
import org.p2psockets.P2PSocket;

import javax.jms.JMSException;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;

/**
 * A JXTA implementation of a TransportChannel
 * 
 * @version $Revision: 1.1 $
 */
public class JxtaTransportChannel extends TcpTransportChannel {

    private static final Log log = LogFactory.getLog(JxtaTransportChannel.class);

    /**
     * Connect to a remote Node - e.g. a Broker
     *
     * @param remoteLocation
     * @throws JMSException
     */
    public JxtaTransportChannel(WireFormat wireFormat, URI remoteLocation) throws JMSException {
        super(wireFormat, remoteLocation);
    }

    /**
     * Connect to a remote Node - e.g. a Broker
     *
     * @param remoteLocation
     * @param localLocation  -
     *                       e.g. local InetAddress and local port
     * @throws JMSException
     */
    public JxtaTransportChannel(WireFormat wireFormat, URI remoteLocation, URI localLocation) throws JMSException {
        super(wireFormat, localLocation, remoteLocation);
    }

    /**
     * pretty print for object
     * 
     * @return String representation of this object
     */
    public String toString() {
        return "P2pTransportChannel: " + socket;
    }

    protected Socket createSocket(URI remoteLocation) throws UnknownHostException, IOException {
        return new P2PSocket(remoteLocation.getHost(), remoteLocation.getPort());
    }

    protected Socket createSocket(URI remoteLocation, URI localLocation) throws IOException, UnknownHostException {
        return new P2PSocket(remoteLocation.getHost(), remoteLocation.getPort(), P2PInetAddress
                .getByName(localLocation.getHost()), localLocation.getPort());
    }

}