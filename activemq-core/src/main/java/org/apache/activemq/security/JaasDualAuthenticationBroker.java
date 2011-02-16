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

package org.apache.activemq.security;

import org.apache.activemq.broker.*;
import org.apache.activemq.broker.jmx.ManagedTransportConnector;
import org.apache.activemq.command.ConnectionInfo;

import org.apache.activemq.transport.tcp.SslTransportServer;

/**
 * A JAAS Authentication Broker that uses different JAAS domain configurations
 * depending if the connection is over an SSL enabled Connector or not.
 *
 * This allows you to, for instance, do DN based authentication for SSL connections
 * and use a mixture of username/passwords and simple guest authentication for
 * non-SSL connections.
 * <p>
 * An example <code>login.config</code> to do do this is:
 * <pre>
 * activemq-domain {
 *   org.apache.activemq.jaas.PropertiesLoginModule sufficient
 *       debug=true
 *       org.apache.activemq.jaas.properties.user="users.properties"
 *       org.apache.activemq.jaas.properties.group="groups.properties";
 *   org.apache.activemq.jaas.GuestLoginModule sufficient
 *       debug=true
 *       org.apache.activemq.jaas.guest.user="guest"
 *       org.apache.activemq.jaas.guest.group="guests";
 * };
 *
 * activemq-ssl-domain {
 *   org.apache.activemq.jaas.TextFileCertificateLoginModule required
 *       debug=true
 *       org.apache.activemq.jaas.textfiledn.user="dns.properties"
 *       org.apache.activemq.jaas.textfiledn.group="groups.properties";
 * };
 * </pre>
 */
public class JaasDualAuthenticationBroker extends BrokerFilter {
    private final JaasCertificateAuthenticationBroker sslBroker;
    private final JaasAuthenticationBroker nonSslBroker;


    /*** Simple constructor. Leaves everything to superclass.
     *
     * @param next The Broker that does the actual work for this Filter.
     * @param jaasConfiguration The JAAS domain configuration name for
     *                non-SSL connections (refer to JAAS documentation).
     * @param jaasSslConfiguration The JAAS domain configuration name for
     *                SSL connections (refer to JAAS documentation).
     */
    public JaasDualAuthenticationBroker(Broker next, String jaasConfiguration, String jaasSslConfiguration) {
        super(next);

        this.nonSslBroker = new JaasAuthenticationBroker(new EmptyBroker(), jaasConfiguration);
        this.sslBroker = new JaasCertificateAuthenticationBroker(new EmptyBroker(), jaasSslConfiguration);
    }

    /**
     * Overridden to allow for authentication using different Jaas
     * configurations depending on if the connection is SSL or not.
     *
     * @param context The context for the incoming Connection.
     * @param info The ConnectionInfo Command representing the incoming
     *                connection.
     */
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        if (context.getSecurityContext() == null) {
            boolean isSSL;
            Connector connector = context.getConnector();
            if (connector instanceof TransportConnector) {
                TransportConnector transportConnector = (TransportConnector) connector;
                isSSL = (transportConnector.getServer() instanceof SslTransportServer);
            } else {
                isSSL = false;
            }

            if (isSSL) {
                this.sslBroker.addConnection(context, info);
            } else {
                this.nonSslBroker.addConnection(context, info);
            }
            super.addConnection(context, info);
        }
    }

    /**
     * Overriding removeConnection to make sure the security context is cleaned.
     */
    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        boolean isSSL;
        Connector connector = context.getConnector();
        if (connector instanceof ManagedTransportConnector) {
            ManagedTransportConnector managedTransportConnector = (ManagedTransportConnector) connector;
            isSSL = (managedTransportConnector.getServer() instanceof SslTransportServer);
        } else {
            isSSL = false;
        }
        super.removeConnection(context, info, error);
        if (isSSL) {
            this.sslBroker.removeConnection(context, info, error);
        } else {
            this.nonSslBroker.removeConnection(context, info, error);
        }
    }
}
