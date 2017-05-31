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

import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.activemq.Service;
import org.apache.activemq.command.BrokerInfo;

/**
 * A TransportServer asynchronously accepts {@see Transport} objects and then
 * delivers those objects to a {@see TransportAcceptListener}.
 *
 *
 */
public interface TransportServer extends Service {

    /**
     * Registers an {@see TransportAcceptListener} which is notified of accepted
     * channels.
     *
     * @param acceptListener
     */
    void setAcceptListener(TransportAcceptListener acceptListener);

    /**
     * Associates a broker info with the transport server so that the transport
     * can do discovery advertisements of the broker.
     *
     * @param brokerInfo
     */
    void setBrokerInfo(BrokerInfo brokerInfo);

    URI getConnectURI();

    /**
     * @return The socket address that this transport is accepting connections
     *         on or null if this does not or is not currently accepting
     *         connections on a socket.
     */
    InetSocketAddress getSocketAddress();

    /**
     * For TransportServers that provide SSL connections to their connected peers they should
     * return true here if and only if they populate the ConnectionInfo command presented to
     * the Broker with the peers certificate chain so that the broker knows it can use that
     * information to authenticate the connected peer.
     *
     * @return true if this transport server provides SSL level security over its
     *          connections.
     */
    boolean isSslServer();

    /**
     * Some protocols allow link stealing by default (if 2 connections have the same clientID - the youngest wins).
     * This is the default for AMQP and MQTT. However, JMS 1.1 spec requires the opposite
     *
     * @return true if allow link stealing is enabled.
     */
    boolean isAllowLinkStealing();
}
