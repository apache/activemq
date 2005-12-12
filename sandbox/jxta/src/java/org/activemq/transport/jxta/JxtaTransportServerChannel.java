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
import org.activemq.transport.tcp.TcpTransportServerChannel;
import org.p2psockets.P2PInetAddress;
import org.p2psockets.P2PServerSocket;

import javax.jms.JMSException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.UnknownHostException;

/**
 * Binds to a well known port and listens for Sockets ...
 *
 * @version $Revision: 1.1 $
 */
public class JxtaTransportServerChannel extends TcpTransportServerChannel {

    private static final Log log = LogFactory.getLog(JxtaTransportServerChannel.class);

    /**
     * Default Constructor
     *
     * @param bindAddr
     * @throws JMSException
     */
    public JxtaTransportServerChannel(WireFormat wireFormat, URI bindAddr) throws JMSException {
        super(wireFormat, bindAddr);
    }

    /**
     * @return pretty print of this
     */
    public String toString() {
        return "P2pTransportServerChannel@" + getUrl();
    }

    protected ServerSocket createServerSocket(URI bind) throws UnknownHostException, IOException {
        ServerSocket answer = null;
        String host = bind.getHost();

        
//        host = (host == null || host.length() == 0) ? "localhost" : host;
//
//        System.out.println("About to lookup host: " + host);
        
        if (host == null || host.length() == 0 || host.equals("localhost")) {
            InetAddress addr = P2PInetAddress.getLocalHost();
            answer = new P2PServerSocket(bind.getPort(), getBacklog(), addr);
        }
        else {
            InetAddress addr = P2PInetAddress.getByName(host);
            answer = new P2PServerSocket(bind.getPort(), getBacklog(), addr);
        }
        /*
        if (addr.equals(P2PInetAddress.getLocalHost())) {
            answer = new P2PServerSocket(bind.getPort(), BACKLOG);
        }
        else {
            answer = new P2PServerSocket(bind.getPort(), BACKLOG, addr);
        }
        */
        //answer = new P2PServerSocket(bind.toString(), BACKLOG);
        return answer;
    }
}