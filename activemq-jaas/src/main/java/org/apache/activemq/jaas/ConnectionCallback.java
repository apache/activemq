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
package org.apache.activemq.jaas;

import java.security.cert.X509Certificate;

import javax.security.auth.callback.Callback;

/**
 * Callback used to pass details of the connection being authenticated to a
 * login module so it can authorize more than the user credentials: the
 * connection id, the requested clientId, the name of the broker performing the
 * authentication, whether the connection has declared itself a network
 * connection, whether it arrived over SSL/TLS, its remote address, the name of
 * the transport connector that accepted it and the client certificate chain it
 * presented, if any.
 */
public class ConnectionCallback implements Callback {

    private String connectionId;
    private String clientId;
    private String brokerName;
    private boolean networkConnection;
    private boolean ssl;
    private String remoteAddress;
    private String transportConnectorName;
    private X509Certificate[] certificates;

    /** the client certificate chain presented during the TLS handshake, or null when none */
    public X509Certificate[] getCertificates() {
        return certificates;
    }

    public void setCertificates(X509Certificate[] certificates) {
        this.certificates = certificates;
    }

    /** true when the connection was accepted over SSL/TLS */
    public boolean isSsl() {
        return ssl;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

    /** the client's remote address as seen by the transport, or null when unknown */
    public String getRemoteAddress() {
        return remoteAddress;
    }

    public void setRemoteAddress(String remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    /** the name of the transport connector that accepted the connection, or null when unknown */
    public String getTransportConnectorName() {
        return transportConnectorName;
    }

    public void setTransportConnectorName(String transportConnectorName) {
        this.transportConnectorName = transportConnectorName;
    }

    /** the connection's id as assigned by the client, or null when unknown */
    public String getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(String connectionId) {
        this.connectionId = connectionId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /** name of the broker authenticating the connection, or null when unknown */
    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    /** true when the connection has identified itself as a network bridge */
    public boolean isNetworkConnection() {
        return networkConnection;
    }

    public void setNetworkConnection(boolean networkConnection) {
        this.networkConnection = networkConnection;
    }
}
