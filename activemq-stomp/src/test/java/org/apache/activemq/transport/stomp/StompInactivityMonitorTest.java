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
package org.apache.activemq.transport.stomp;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that the inactivity monitor works as expected.
 */
@RunWith(Parameterized.class)
public class StompInactivityMonitorTest extends StompTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(StompInactivityMonitorTest.class);

    private final String transportScheme;

    @Parameters(name="{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { "stomp" },
                { "stomp+ssl" },
                { "stomp+nio" },
                { "stomp+nio+ssl" }
            });
    }

    public StompInactivityMonitorTest(String transportScheme) {
        this.transportScheme = transportScheme;
    }

    @Test
    public void test() throws Exception {
        stompConnect();

        String connectFrame = "STOMP\n" +
            "login:system\n" +
            "passcode:manager\n" +
            "accept-version:1.1\n" +
            "heart-beat:1000,0\n" +
            "host:localhost\n" +
            "\n" + Stomp.NULL;

        stompConnection.sendFrame(connectFrame);
        String response = stompConnection.receiveFrame().trim();
        LOG.info("Broker sent response: {}", response);

        String messageHead = "SEND\n" +
                             "receipt:1" + "\n" +
                             "destination:/queue/" + getQueueName() +
                             "\n\n" + "AAAA";

        stompConnection.sendFrame(messageHead);

        for (int i = 0; i < 30; i++) {
            stompConnection.sendFrame("A");
            Thread.sleep(100);
        }

        stompConnection.sendFrame(Stomp.NULL);

        response = stompConnection.receiveFrame().trim();
        assertTrue(response.startsWith("RECEIPT"));
    }

    @Override
    protected boolean isUseTcpConnector() {
        return !transportScheme.contains("nio") && !transportScheme.contains("ssl");
    }

    @Override
    protected boolean isUseSslConnector() {
        return !transportScheme.contains("nio") && transportScheme.contains("ssl");
    }

    @Override
    protected boolean isUseNioConnector() {
        return transportScheme.contains("nio") && !transportScheme.contains("ssl");
    }

    @Override
    protected boolean isUseNioPlusSslConnector() {
        return transportScheme.contains("nio") && transportScheme.contains("ssl");
    }

    @Override
    protected Socket createSocket() throws IOException {
        int port = 0;
        boolean useSSL = false;

        if (transportScheme.contains("ssl")) {
            if (transportScheme.contains("nio")) {
                port = this.nioSslPort;
            } else {
                port = this.sslPort;
            }

            useSSL = true;
        } else {
            if (transportScheme.contains("nio")) {
                port = this.nioPort;
            } else {
                port = this.port;
            }
        }

        SocketFactory factory = null;

        if (useSSL) {
            factory = SSLSocketFactory.getDefault();
        } else {
            factory = SocketFactory.getDefault();
        }

        return factory.createSocket("127.0.0.1", port);
    }
}
