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
package org.apache.activemq.transport;

import java.net.InetAddress;
import java.net.URI;
import java.util.Map;

import org.apache.activemq.util.InetAddressUtil;
import org.apache.activemq.util.IntrospectionSupport;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.security.Constraint;

abstract public class WebTransportServerSupport extends TransportServerSupport {

    protected URI bindAddress;
    protected Server server;
    protected Connector connector;
    protected SocketConnectorFactory socketConnectorFactory;
    protected String host;
    protected final HttpOptions httpOptions = new HttpOptions();

    public WebTransportServerSupport(URI location) {
        super(location);
    }

    private <T> void setConnectorProperty(String name, Class<T> type, T value) throws Exception {
        connector.getClass().getMethod("set" + name, type).invoke(connector, value);
    }

    protected void createServer() {
        server = new Server();
        try {
            server.getClass().getMethod("setStopTimeout", Long.TYPE).invoke(server, 500l);
        } catch (Throwable t) {
            //ignore, jetty 8.
        }
    }

    public URI bind() throws Exception {

        URI bind = getBindLocation();

        String bindHost = bind.getHost();
        bindHost = (bindHost == null || bindHost.length() == 0) ? "localhost" : bindHost;
        InetAddress addr = InetAddress.getByName(bindHost);
        host = addr.getCanonicalHostName();

        setConnectorProperty("Host", String.class, host);
        setConnectorProperty("Port", Integer.TYPE, bindAddress.getPort());
        server.addConnector(connector);
        if (addr.isAnyLocalAddress()) {
            host = InetAddressUtil.getLocalHostName();
        }

        URI boundUri = new URI(bind.getScheme(), bind.getUserInfo(), host, bindAddress.getPort(), bind.getPath(), bind.getQuery(), bind.getFragment());
        setConnectURI(boundUri);
        return boundUri;
    }

    protected void configureTraceMethod(ConstraintSecurityHandler securityHandler,
            boolean enableTrace) {
        Constraint constraint = new Constraint();
        constraint.setName("trace-security");
        //If enableTrace is true, then we want to set authenticate to false to allow it
        constraint.setAuthenticate(!enableTrace);
        ConstraintMapping mapping = new ConstraintMapping();
        mapping.setConstraint(constraint);
        mapping.setMethod("TRACE");
        mapping.setPathSpec("/");
        securityHandler.addConstraintMapping(mapping);
    }

    public void setHttpOptions(Map<String, Object> options) {
        if (options != null) {
            IntrospectionSupport.setProperties(this.httpOptions, options);
        }
    }

    protected static class HttpOptions {
        private boolean enableTrace = false;

        public boolean isEnableTrace() {
            return enableTrace;
        }

        public void setEnableTrace(boolean enableTrace) {
            this.enableTrace = enableTrace;
        }
    }
}
