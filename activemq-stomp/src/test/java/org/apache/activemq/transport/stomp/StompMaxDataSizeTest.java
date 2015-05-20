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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import org.junit.Test;

public class StompMaxDataSizeTest extends StompTestSupport {

    private static final int TEST_MAX_DATA_SIZE = 64 * 1024;

    private StompConnection connection;

    @Override
    public void tearDown() throws Exception {
        if (connection != null) {
            try {
                connection.close();
            } catch (Throwable ex) {}
        }
        super.tearDown();
    }

    @Override
    protected boolean isUseSslConnector() {
        return true;
    }

    @Override
    protected boolean isUseNioConnector() {
        return true;
    }

    @Override
    protected boolean isUseNioPlusSslConnector() {
        return true;
    }

    @Override
    protected String getAdditionalConfig() {
        return "?transport.maxDataLength=" + TEST_MAX_DATA_SIZE;
    }

    @Test(timeout = 60000)
    public void testOversizedMessageOnPlainSocket() throws Exception {
        doTestOversizedMessage(port, false);
    }

    @Test(timeout = 60000)
    public void testOversizedMessageOnNioSocket() throws Exception {
        doTestOversizedMessage(nioPort, false);
    }

    @Test//(timeout = 60000)
    public void testOversizedMessageOnSslSocket() throws Exception {
        doTestOversizedMessage(sslPort, true);
    }

    @Test(timeout = 60000)
    public void testOversizedMessageOnNioSslSocket() throws Exception {
        doTestOversizedMessage(nioSslPort, true);
    }

    protected void doTestOversizedMessage(int port, boolean useSsl) throws Exception {
        stompConnect(port, useSsl);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        int size = 100;
        char[] bigBodyArray = new char[size];
        Arrays.fill(bigBodyArray, 'a');
        String bigBody = new String(bigBodyArray);

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + bigBody + Stomp.NULL;

        stompConnection.sendFrame(frame);

        StompFrame received = stompConnection.receive();
        assertNotNull(received);
        assertEquals("MESSAGE", received.getAction());
        assertEquals(bigBody, received.getBody());

        size = TEST_MAX_DATA_SIZE + 100;
        bigBodyArray = new char[size];
        Arrays.fill(bigBodyArray, 'a');
        bigBody = new String(bigBodyArray);

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + bigBody + Stomp.NULL;

        stompConnection.sendFrame(frame);

        received = stompConnection.receive(5000);
        assertNotNull(received);
        assertEquals("ERROR", received.getAction());
    }

    protected StompConnection stompConnect(int port, boolean ssl) throws Exception {
        if (stompConnection == null) {
            stompConnection = new StompConnection();
        }

        Socket socket = null;
        if (ssl) {
            socket = createSslSocket(port);
        } else {
            socket = createSocket(port);
        }

        stompConnection.open(socket);

        return stompConnection;
    }

    protected Socket createSocket(int port) throws IOException {
        return new Socket("127.0.0.1", port);
    }

    protected Socket createSslSocket(int port) throws IOException {
        SocketFactory factory = SSLSocketFactory.getDefault();
        return factory.createSocket("127.0.0.1", port);
    }
}
