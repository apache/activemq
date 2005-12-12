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
package org.activemq.transport.http;

import org.activemq.command.BrokerInfo;
import org.activemq.transport.TransportServerSupport;
import org.activemq.transport.util.TextWireFormat;
import org.activemq.transport.xstream.XStreamWireFormat;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.handler.ContextHandler;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.servlet.ServletMapping;
import org.mortbay.jetty.servlet.SessionHandler;

import java.net.URI;

/**
 * @version $Revision$
 */
public class HttpTransportServer extends TransportServerSupport {
    private URI bindAddress;
    private TextWireFormat wireFormat;
    private Server server;
    private Connector connector;

    public HttpTransportServer(URI uri) {
        super(uri);
        this.bindAddress = uri;
    }

    public void start() throws Exception {
        server = new Server();
        if (connector==null)
            connector = new SocketConnector();
        connector.setHost(bindAddress.getHost());
        connector.setPort(bindAddress.getPort());
        connector.setServer(server);
        server.setConnectors(new Connector[] { connector });

        ContextHandler context_handler = new ContextHandler();
        context_handler.setContextPath("/");
        context_handler.setServer(server);
        server.setHandler(context_handler);

        SessionHandler session_handler = new SessionHandler();
        context_handler.setHandler(session_handler);
        
        ServletHandler servlet_handler = new ServletHandler();
        session_handler.setHandler(servlet_handler);

        ServletHolder holder = new ServletHolder();
        holder.setName("httpTunnel");
        holder.setClassName(HttpTunnelServlet.class.getName());
        servlet_handler.setServlets(new ServletHolder[] { holder });

        ServletMapping mapping = new ServletMapping();
        mapping.setServletName("httpTunnel");
        mapping.setPathSpec("/*");
        servlet_handler.setServletMappings(new ServletMapping[] { mapping });

        context_handler.setAttribute("acceptListener", getAcceptListener());
        context_handler.setAttribute("wireFormat", getWireFormat());
        server.start();
    }

    public synchronized void stop() throws Exception {
        Server temp = server;
        server = null;
        if (temp != null) {
            temp.stop();
        }
    }

    // Properties
    // -------------------------------------------------------------------------
    public TextWireFormat getWireFormat() {
        if (wireFormat == null) {
            wireFormat = createWireFormat();
        }
        return wireFormat;
    }

    public void setWireFormat(TextWireFormat wireFormat) {
        this.wireFormat = wireFormat;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected TextWireFormat createWireFormat() {
        return new XStreamWireFormat();
    }

    protected void setConnector(Connector connector) {
        this.connector = connector;
    }

    public void setBrokerInfo(BrokerInfo brokerInfo) {
    }
}
