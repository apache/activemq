/*
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
package org.apache.activemq.util;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLSocket;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.nio.NIOSSLTransport;

public class NioSslTestUtil {

    public static void checkHandshakeStatusAdvances(TransportConnector connector, SSLSocket socket) throws Exception {
        checkHandshakeStatusAdvances(connector, socket.getLocalPort(), 10000, 10);
    }

    public static void checkHandshakeStatusAdvances(TransportConnector connector, int localPort) throws Exception {
        checkHandshakeStatusAdvances(connector, localPort, 10000, 10);
    }

    public static void checkHandshakeStatusAdvances(TransportConnector connector, int localPort,
            long duration, long sleepMillis) throws Exception {

        TransportConnection con = connector.getConnections().stream()
                .filter(tc -> tc.getRemoteAddress().contains(
                Integer.toString(localPort))).findFirst().orElseThrow();

        Field field = NIOSSLTransport.class.getDeclaredField("handshakeStatus");
        field.setAccessible(true);
        NIOSSLTransport t = con.getTransport().narrow(NIOSSLTransport.class);
        // If this is the NIOSSLTransport then verify we exit NEED_WRAP and NEED_TASK
        if (t != null) {
            assertTrue(Wait.waitFor(() -> {
                SSLEngineResult.HandshakeStatus status = (SSLEngineResult.HandshakeStatus) field.get(t);
                return status != HandshakeStatus.NEED_WRAP && status != HandshakeStatus.NEED_TASK;
            }, duration, sleepMillis));
        }
    }
}
