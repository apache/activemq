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

package org.apache.activemq.transport.ws;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Map;

import javax.servlet.Servlet;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.transport.SocketConnectorFactory;
import org.apache.activemq.transport.WebTransportServerSupport;
import org.apache.activemq.transport.ws.jetty9.WSServlet;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a web server and registers web socket server
 *
 */
public class WSTransportServer extends WebTransportServerSupport implements BrokerServiceAware {

    private static final Logger LOG = LoggerFactory.getLogger(WSTransportServer.class);

    private BrokerService brokerService;
    private WSServlet servlet;

    public WSTransportServer(URI location) {
        super(location);
        this.bindAddress = location;
        socketConnectorFactory = new SocketConnectorFactory();
    }

    @Override
    protected void doStart() throws Exception {
        createServer();

        if (connector == null) {
            connector = socketConnectorFactory.createConnector(server);
        }

        for(ConnectionFactory cf  : connector.getConnectionFactories()) {
            if(HttpConnectionFactory.class.isAssignableFrom(cf.getClass())) {
                HttpConnectionFactory.class.cast(cf).getHttpConfiguration().setSendServerVersion(false);
            }
        }

        URI boundTo = bind();

        ServletContextHandler contextHandler =
                new ServletContextHandler(server, "/", ServletContextHandler.SECURITY);

        ServletHolder holder = new ServletHolder();

        //AMQ-6182 - disabling trace by default
        configureTraceMethod((ConstraintSecurityHandler) contextHandler.getSecurityHandler(),
                httpOptions.isEnableTrace());

        Map<String, Object> webSocketOptions = IntrospectionSupport.extractProperties(transportOptions, "websocket.");
        for(Map.Entry<String,Object> webSocketEntry : webSocketOptions.entrySet()) {
            Object value = webSocketEntry.getValue();
            if (value != null) {
                holder.setInitParameter(webSocketEntry.getKey(), value.toString());
            }
        }

        holder.setServlet(createWSServlet());
        contextHandler.addServlet(holder, "/");

        contextHandler.setAttribute("acceptListener", getAcceptListener());

        server.start();

        // Update the Connect To URI with our actual location in case the configured port
        // was set to zero so that we report the actual port we are listening on.

        int port = getConnectorLocalPort();
        if (port == -1) {
            port = boundTo.getPort();
        }

        setConnectURI(new URI(boundTo.getScheme(),
                              boundTo.getUserInfo(),
                              boundTo.getHost(),
                              port,
                              boundTo.getPath(),
                              boundTo.getQuery(),
                              boundTo.getFragment()));

        LOG.info("Listening for connections at {}", getConnectURI());
    }

    private Servlet createWSServlet() throws Exception {
        servlet = new WSServlet();
        servlet.setTransportOptions(transportOptions);
        servlet.setBrokerService(brokerService);

        return servlet;
    }

    private int getConnectorLocalPort() throws Exception {
        return (Integer)connector.getClass().getMethod("getLocalPort").invoke(connector);
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        Server temp = server;
        server = null;
        if (temp != null) {
            temp.stop();
        }
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return null;
    }

    @Override
    public void setBrokerInfo(BrokerInfo brokerInfo) {
    }

    protected void setConnector(Connector connector) {
        this.connector = connector;
    }

    @Override
    public void setTransportOption(Map<String, Object> transportOptions) {
        // String transport from options and
        Map<String, Object> socketOptions = IntrospectionSupport.extractProperties(transportOptions, "transport.");
        socketConnectorFactory.setTransportOptions(socketOptions);
        transportOptions.putAll(socketOptions);
        super.setTransportOption(transportOptions);
    }

    @Override
    public boolean isSslServer() {
        return false;
    }

    @Override
    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
        if (servlet != null) {
            servlet.setBrokerService(brokerService);
        }
    }

    @Override
    public long getMaxConnectionExceededCount() {
        // Max Connection Count not supported for ws
        return -1l;
    }

    @Override
    public void resetStatistics() {
        // Statistics not implemented for ws
    }
}
