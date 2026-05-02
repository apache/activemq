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

import java.net.Socket;

/**
 * An admission policy a transport server applies to each accepted socket.
 *
 * <p>When a TransportConnector is configured with a TransportConnectorPolicy, the
 * process method is invoked with each accepted Socket before any protocol
 * negotiation, connection counting or transport setup.
 *
 * <p>Implementations may inspect the socket and its remote address and throw a
 * {@link SecurityException} to refuse it. The server closes a refused socket
 * without writing anything to it and continues accepting.
 *
 * <p>Implementations must be thread safe: sockets from all acceptor threads of
 * a server pass through a single instance. An implementation may also delegate
 * to other policies, so several checks can run against one connector in order.
 */
public interface TransportConnectorPolicy {

    /**
     * @param socket an accepted socket, connected and not yet handed to a transport
     * @throws SecurityException to refuse the socket
     */
    void process(Socket socket) throws SecurityException;

}
