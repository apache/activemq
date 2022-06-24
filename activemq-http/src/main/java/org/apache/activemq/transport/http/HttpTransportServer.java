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
package org.apache.activemq.transport.http;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.transport.SocketConnectorFactory;
import org.apache.activemq.transport.WebTransportServerSupport;
import org.apache.activemq.transport.util.TextWireFormat;
import org.apache.activemq.transport.xstream.XStreamWireFormat;
import org.apache.activemq.util.ServiceStopper;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class HttpTransportServer extends WebTransportServerSupport {

    private TextWireFormat wireFormat;
    private final HttpTransportFactory transportFactory;
    private Map<String, Object> wireFormatOptions = new HashMap<>();

    public HttpTransportServer(URI uri, HttpTransportFactory factory) {
        super(uri);
        this.bindAddress = uri;
        this.transportFactory = factory;
        socketConnectorFactory = new SocketConnectorFactory();
    }

    @Override
    public void setBrokerInfo(BrokerInfo brokerInfo) {
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
        holder.setServlet(new HttpTunnelServlet());
        contextHandler.addServlet(holder, "/");

        contextHandler.setAttribute("acceptListener", getAcceptListener());
        contextHandler.setAttribute("wireFormat", getWireFormat());
        contextHandler.setAttribute("transportFactory", transportFactory);
        contextHandler.setAttribute("transportOptions", transportOptions);
        contextHandler.setAttribute("wireFormatOptions", wireFormatOptions);

        //AMQ-6182 - disabling trace by default
        configureTraceMethod((ConstraintSecurityHandler) contextHandler.getSecurityHandler(),
                httpOptions.isEnableTrace());

        addGzipHandler(contextHandler);

        server.start();

        // Update the Connect To URI with our actual location in case the configured port
        // was set to zero so that we report the actual port we are listening on.

        int port = boundTo.getPort();
        int p2 = getConnectorLocalPort();
        if (p2 != -1) {
            port = p2;
        }

        setConnectURI(new URI(boundTo.getScheme(),
                              boundTo.getUserInfo(),
                              boundTo.getHost(),
                              port,
                              boundTo.getPath(),
                              boundTo.getQuery(),
                              boundTo.getFragment()));
    }

    private int getConnectorLocalPort() throws Exception {
        return (Integer)connector.getClass().getMethod("getLocalPort").invoke(connector);
    }

    private void addGzipHandler(ServletContextHandler contextHandler) throws Exception {
        HandlerWrapper handler = null;
        try {
            handler = (HandlerWrapper) forName("org.eclipse.jetty.servlets.gzip.GzipHandler").getConstructor().newInstance();
        } catch (Throwable t) {
            handler = (HandlerWrapper) forName("org.eclipse.jetty.server.handler.gzip.GzipHandler").getConstructor().newInstance();
        }
        contextHandler.insertHandler(handler);
    }

    private Class<?> forName(String name) throws ClassNotFoundException {
        Class<?> clazz = null;
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader != null) {
            try {
                clazz = loader.loadClass(name);
            } catch (ClassNotFoundException e) {
                // ignore
            }
        }
        if (clazz == null) {
            clazz = HttpTransportServer.class.getClassLoader().loadClass(name);
        }

        return clazz;
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
    public void setTransportOption(Map<String, Object> transportOptions) {
        socketConnectorFactory.setTransportOptions(transportOptions);
        super.setTransportOption(transportOptions);
    }

    public void setWireFormatOptions(Map<String, Object> wireFormatOptions) {
        this.wireFormatOptions = wireFormatOptions;
    }

    @Override
    public boolean isSslServer() {
        return false;
    }

    @Override
    public long getMaxConnectionExceededCount() {
        // Max Connection Count not supported for http
        return -1l;
    }

    @Override
    public void resetStatistics() {
        // Statistics not implemented for http
    }
}
