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
import java.util.Collection;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class StompMaxFrameSizeTest extends StompTestSupport {

    enum TestType {FRAME_MAX_GREATER_THAN_HEADER_MAX, FRAME_MAX_LESS_THAN_HEADER_MAX, FRAME_MAX_LESS_THAN_ACTION_MAX};

    // set max data size higher than max frame size so that max frame size gets tested
    private static final int MAX_DATA_SIZE = 100 * 1024;
    private final TestType testType;
    private final int maxFrameSize;

    /**
     * This defines the different possible max header sizes for this test.
     */
    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                // The maximum size exceeds the default max header size of 10 * 1024
                {TestType.FRAME_MAX_GREATER_THAN_HEADER_MAX, 64 * 1024},
                // The maximum size is less than the default max header size of 10 * 1024
                {TestType.FRAME_MAX_LESS_THAN_HEADER_MAX, 5 * 1024},
                // The maximum size is less than the default max action size of 1024
                {TestType.FRAME_MAX_LESS_THAN_ACTION_MAX, 512}
        });
    }

    public StompMaxFrameSizeTest(TestType testType, int maxFrameSize) {
        this.testType = testType;
        this.maxFrameSize = maxFrameSize;
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
        return "?wireFormat.maxDataLength=" + MAX_DATA_SIZE + "&wireFormat.maxFrameSize=" + maxFrameSize;
    }

    /**
     * These tests should cause a Stomp error because the body size is greater than the
     * max allowed frame size
     */

    @Test(timeout = 60000)
    public void testOversizedBodyOnPlainSocket() throws Exception {
        Assume.assumeTrue(testType == TestType.FRAME_MAX_GREATER_THAN_HEADER_MAX);
        doOversizedTestMessage(port, false, maxFrameSize + 100);
    }

    @Test(timeout = 60000)
    public void testOversizedBodyOnNioSocket() throws Exception {
        Assume.assumeTrue(testType == TestType.FRAME_MAX_GREATER_THAN_HEADER_MAX);
        doOversizedTestMessage(nioPort, false, maxFrameSize + 100);
    }

    @Test(timeout = 60000)
    public void testOversizedBodyOnSslSocket() throws Exception {
        Assume.assumeTrue(testType == TestType.FRAME_MAX_GREATER_THAN_HEADER_MAX);
        doOversizedTestMessage(sslPort, true, maxFrameSize + 100);
    }

    @Test(timeout = 60000)
    public void testOversizedBodyOnNioSslSocket() throws Exception {
        Assume.assumeTrue(testType == TestType.FRAME_MAX_GREATER_THAN_HEADER_MAX);
        doOversizedTestMessage(nioSslPort, true, maxFrameSize + 100);
    }

    /**
     * These tests should cause a Stomp error because even though the body size is less than max frame size,
     * the action and headers plus data size should cause a max frame size failure
     */
    @Test(timeout = 60000)
    public void testOversizedTotalFrameOnPlainSocket() throws Exception {
        Assume.assumeTrue(testType == TestType.FRAME_MAX_GREATER_THAN_HEADER_MAX);
        doOversizedTestMessage(port, false, maxFrameSize - 50);
    }

    @Test(timeout = 60000)
    public void testOversizedTotalFrameOnNioSocket() throws Exception {
        Assume.assumeTrue(testType == TestType.FRAME_MAX_GREATER_THAN_HEADER_MAX);
        doOversizedTestMessage(nioPort, false, maxFrameSize - 50);
    }

    @Test(timeout = 60000)
    public void testOversizedTotalFrameOnSslSocket() throws Exception {
        Assume.assumeTrue(testType == TestType.FRAME_MAX_GREATER_THAN_HEADER_MAX);
        doOversizedTestMessage(sslPort, true, maxFrameSize - 50);
    }

    @Test(timeout = 60000)
    public void testOversizedTotalFrameOnNioSslSocket() throws Exception {
        Assume.assumeTrue(testType == TestType.FRAME_MAX_GREATER_THAN_HEADER_MAX);
        doOversizedTestMessage(nioSslPort, true, maxFrameSize - 50);
    }

    /**
     * These tests will test a successful Stomp message when the total size is than max frame size
     */
    @Test(timeout = 60000)
    public void testUndersizedTotalFrameOnPlainSocket() throws Exception {
        Assume.assumeTrue(testType == TestType.FRAME_MAX_GREATER_THAN_HEADER_MAX);
        doUndersizedTestMessage(port, false);
    }

    @Test(timeout = 60000)
    public void testUndersizedTotalFrameOnNioSocket() throws Exception {
        Assume.assumeTrue(testType == TestType.FRAME_MAX_GREATER_THAN_HEADER_MAX);
        doUndersizedTestMessage(nioPort, false);
    }

    @Test(timeout = 60000)
    public void testUndersizedTotalFrameOnSslSocket() throws Exception {
        Assume.assumeTrue(testType == TestType.FRAME_MAX_GREATER_THAN_HEADER_MAX);
        doUndersizedTestMessage(sslPort, true);
    }

    @Test(timeout = 60000)
    public void testUndersizedTotalFrameOnNioSslSocket() throws Exception {
        Assume.assumeTrue(testType == TestType.FRAME_MAX_GREATER_THAN_HEADER_MAX);
        doUndersizedTestMessage(nioSslPort, true);
    }

    /**
     *  These tests test that a Stomp error occurs if the action size exceeds maxFrameSize
     *  when the maxFrameSize length is less than the default max action length
     */

    @Test(timeout = 60000)
    public void testOversizedActionOnPlainSocket() throws Exception {
        Assume.assumeTrue(testType == TestType.FRAME_MAX_LESS_THAN_ACTION_MAX);
        doTestOversizedAction(port, false);
    }

    @Test(timeout = 60000)
    public void testOversizedActionOnNioSocket() throws Exception {
        Assume.assumeTrue(testType == TestType.FRAME_MAX_LESS_THAN_ACTION_MAX);
        doTestOversizedAction(nioPort, false);
    }

    @Test(timeout = 60000)
    public void testOversizedActionOnSslSocket() throws Exception {
        Assume.assumeTrue(testType == TestType.FRAME_MAX_LESS_THAN_ACTION_MAX);
        doTestOversizedAction(sslPort, true);
    }

    @Test(timeout = 60000)
    public void testOversizedActionOnNioSslSocket() throws Exception {
        Assume.assumeTrue(testType == TestType.FRAME_MAX_LESS_THAN_ACTION_MAX);
        doTestOversizedAction(nioSslPort, true);
    }


    /**
     *  These tests will test that a Stomp error occurs if the header size exceeds maxFrameSize
     *  when the maxFrameSize length is less than the default max header length
     */
    @Test(timeout = 60000)
    public void testOversizedHeadersOnPlainSocket() throws Exception {
        Assume.assumeTrue(testType == TestType.FRAME_MAX_LESS_THAN_HEADER_MAX);
        doTestOversizedHeaders(port, false);
    }

    @Test(timeout = 60000)
    public void testOversizedHeadersOnNioSocket() throws Exception {
        Assume.assumeTrue(testType == TestType.FRAME_MAX_LESS_THAN_HEADER_MAX);
        doTestOversizedHeaders(nioPort, false);
    }

    @Test(timeout = 60000)
    public void testOversizedHeadersOnSslSocket() throws Exception {
        Assume.assumeTrue(testType == TestType.FRAME_MAX_LESS_THAN_HEADER_MAX);
        doTestOversizedHeaders(sslPort, true);
    }

    @Test(timeout = 60000)
    public void testOversizedHeadersOnNioSslSocket() throws Exception {
        Assume.assumeTrue(testType == TestType.FRAME_MAX_LESS_THAN_HEADER_MAX);
        doTestOversizedHeaders(nioSslPort, true);
    }

    protected void doTestOversizedAction(int port, boolean useSsl) throws Exception {
        initializeStomp(port, useSsl);

        char[] actionArray = new char[maxFrameSize + 100];
        Arrays.fill(actionArray, 'A');
        String action = new String(actionArray);

        String frame = action + "\n" + "destination:/queue/" + getQueueName() + "\n\n" + "body" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        StompFrame received = stompConnection.receive(500000);
        assertNotNull(received);
        assertEquals("ERROR", received.getAction());
        assertTrue(received.getBody().contains("maximum frame size"));
    }

    protected void doTestOversizedHeaders(int port, boolean useSsl) throws Exception {
        initializeStomp(port, useSsl);

        StringBuilder headers = new StringBuilder(maxFrameSize + 100);
        int i = 0;
        while (headers.length() < maxFrameSize + 1) {
            headers.append("key" + i++ + ":value\n");
        }

        String frame = "SEND\n" + headers.toString() + "\n" + "destination:/queue/" + getQueueName() +
                headers.toString() + "\n\n" + "body" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        StompFrame received = stompConnection.receive(5000);
        assertNotNull(received);
        assertEquals("ERROR", received.getAction());
        assertTrue(received.getBody().contains("maximum frame size"));
    }

    protected void doOversizedTestMessage(int port, boolean useSsl, int dataSize) throws Exception {
        initializeStomp(port, useSsl);

        int size = dataSize + 100;
        char[] bigBodyArray = new char[size];
        Arrays.fill(bigBodyArray, 'a');
        String bigBody = new String(bigBodyArray);

        String frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + bigBody + Stomp.NULL;

        stompConnection.sendFrame(frame);

        StompFrame received = stompConnection.receive(5000);
        assertNotNull(received);
        assertEquals("ERROR", received.getAction());
        assertTrue(received.getBody().contains("maximum frame size"));
    }

    protected void doUndersizedTestMessage(int port, boolean useSsl) throws Exception {
        initializeStomp(port, useSsl);

        int size = 100;
        char[] bigBodyArray = new char[size];
        Arrays.fill(bigBodyArray, 'a');
        String bigBody = new String(bigBodyArray);

        String frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n\n" + bigBody + Stomp.NULL;

        stompConnection.sendFrame(frame);

        StompFrame received = stompConnection.receive();
        assertNotNull(received);
        assertEquals("MESSAGE", received.getAction());
        assertEquals(bigBody, received.getBody());
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

    protected void initializeStomp(int port, boolean useSsl) throws Exception{
        stompConnect(port, useSsl);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + getQueueName() + "\n" + "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }

    protected Socket createSocket(int port) throws IOException {
        return new Socket("127.0.0.1", port);
    }

    protected Socket createSslSocket(int port) throws IOException {
        SocketFactory factory = SSLSocketFactory.getDefault();
        return factory.createSocket("127.0.0.1", port);
    }
}
