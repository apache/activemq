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
package org.apache.activemq.transport.amqp.client;

import java.io.IOException;

/**
 * {@link IOException} derivative that defines that the remote peer has requested that this
 * connection be redirected to some alternative peer.
 */
public class AmqpRedirectedException extends IOException {

    private static final long serialVersionUID = 5872211116061710369L;

    private final String hostname;
    private final String networkHost;
    private final int port;

    public AmqpRedirectedException(String reason, String hostname, String networkHost, int port) {
        super(reason);

        this.hostname = hostname;
        this.networkHost = networkHost;
        this.port = port;
    }

    /**
     * @return the host name of the container being redirected to.
     */
    public String getHostname() {
        return hostname;
    }

    /**
     * @return the DNS host name or IP address of the peer this connection is being redirected to.
     */
    public String getNetworkHost() {
        return networkHost;
    }

    /**
     * @return the port number on the peer this connection is being redirected to.
     */
    public int getPort() {
        return port;
    }
}
