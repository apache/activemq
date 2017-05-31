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
package org.apache.activemq.transport.amqp.protocol;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import org.apache.activemq.transport.amqp.AmqpHeader;
import org.apache.activemq.transport.amqp.AmqpTestSupport;
import org.apache.activemq.util.Wait;
import org.fusesource.hawtbuf.Buffer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that the Broker handles connections from older clients or
 * non-AMQP client correctly by returning an AMQP header prior to
 * closing the socket.
 */
public class UnsupportedClientTest extends AmqpTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(UnsupportedClientTest.class);

    @Override
    @Before
    public void setUp() throws Exception {
        System.setProperty("javax.net.ssl.trustStore", "src/test/resources/client.keystore");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStoreType", "jks");
        System.setProperty("javax.net.ssl.keyStore", "src/test/resources/server.keystore");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.keyStoreType", "jks");

        super.setUp();
    }

    @Override
    public String getAdditionalConfig() {
        return "&wireFormat.allowNonSaslConnections=false";
    }

    @Test(timeout = 60000)
    public void testOlderProtocolIsRejected() throws Exception {

        AmqpHeader header = new AmqpHeader();

        header.setProtocolId(3);
        header.setMajor(0);
        header.setMinor(9);
        header.setRevision(1);

        // Test TCP
        doTestInvalidHeaderProcessing(amqpPort, header, false);

        // Test SSL
        doTestInvalidHeaderProcessing(amqpSslPort, header, true);

        // Test NIO
        doTestInvalidHeaderProcessing(amqpNioPort, header, false);

        // Test NIO+SSL
        doTestInvalidHeaderProcessing(amqpNioPlusSslPort, header, true);
    }

    @Test(timeout = 60000)
    public void testNewerMajorIsRejected() throws Exception {

        AmqpHeader header = new AmqpHeader();

        header.setProtocolId(3);
        header.setMajor(2);
        header.setMinor(0);
        header.setRevision(0);

        // Test TCP
        doTestInvalidHeaderProcessing(amqpPort, header, false);

        // Test SSL
        doTestInvalidHeaderProcessing(amqpSslPort, header, true);

        // Test NIO
        doTestInvalidHeaderProcessing(amqpNioPort, header, false);

        // Test NIO+SSL
        doTestInvalidHeaderProcessing(amqpNioPlusSslPort, header, true);
    }

    @Test(timeout = 60000)
    public void testNewerMinorIsRejected() throws Exception {

        AmqpHeader header = new AmqpHeader();

        header.setProtocolId(3);
        header.setMajor(1);
        header.setMinor(1);
        header.setRevision(0);

        // Test TCP
        doTestInvalidHeaderProcessing(amqpPort, header, false);

        // Test SSL
        doTestInvalidHeaderProcessing(amqpSslPort, header, true);

        // Test NIO
        doTestInvalidHeaderProcessing(amqpNioPort, header, false);

        // Test NIO+SSL
        doTestInvalidHeaderProcessing(amqpNioPlusSslPort, header, true);
    }

    @Test(timeout = 60000)
    public void testNewerRevisionIsRejected() throws Exception {

        AmqpHeader header = new AmqpHeader();

        header.setProtocolId(3);
        header.setMajor(1);
        header.setMinor(0);
        header.setRevision(1);

        // Test TCP
        doTestInvalidHeaderProcessing(amqpPort, header, false);

        // Test SSL
        doTestInvalidHeaderProcessing(amqpSslPort, header, true);

        // Test NIO
        doTestInvalidHeaderProcessing(amqpNioPort, header, false);

        // Test NIO+SSL
        doTestInvalidHeaderProcessing(amqpNioPlusSslPort, header, true);
    }

    @Test(timeout = 60000)
    public void testNonSaslClientIsRejected() throws Exception {

        AmqpHeader header = new AmqpHeader();

        header.setProtocolId(0);
        header.setMajor(1);
        header.setMinor(0);
        header.setRevision(0);

        // Test TCP
        doTestInvalidHeaderProcessing(amqpPort, header, false);

        // Test SSL
        doTestInvalidHeaderProcessing(amqpSslPort, header, true);

        // Test NIO
        doTestInvalidHeaderProcessing(amqpNioPort, header, false);

        // Test NIO+SSL
        doTestInvalidHeaderProcessing(amqpNioPlusSslPort, header, true);
    }

    @Test(timeout = 60000)
    public void testUnkownProtocolIdIsRejected() throws Exception {

        AmqpHeader header = new AmqpHeader();

        header.setProtocolId(5);
        header.setMajor(1);
        header.setMinor(0);
        header.setRevision(0);

        // Test TCP
        doTestInvalidHeaderProcessing(amqpPort, header, false);

        // Test SSL
        doTestInvalidHeaderProcessing(amqpSslPort, header, true);

        // Test NIO
        doTestInvalidHeaderProcessing(amqpNioPort, header, false);

        // Test NIO+SSL
        doTestInvalidHeaderProcessing(amqpNioPlusSslPort, header, true);
    }

    @Test(timeout = 60000)
    public void testInvalidProtocolHeader() throws Exception {

        AmqpHeader header = new AmqpHeader(new Buffer(new byte[]{'S', 'T', 'O', 'M', 'P', 0, 0, 0}), false);

        // Test TCP
        doTestInvalidHeaderProcessing(amqpPort, header, false);

        // Test SSL
        doTestInvalidHeaderProcessing(amqpSslPort, header, true);

        // Test NIO
        doTestInvalidHeaderProcessing(amqpNioPort, header, false);

        // Test NIO+SSL
        doTestInvalidHeaderProcessing(amqpNioPlusSslPort, header, true);
    }

    protected void doTestInvalidHeaderProcessing(int port, final AmqpHeader header, boolean ssl) throws Exception {
        final ClientConnection connection = createClientConnection(ssl);
        connection.open("localhost", port);
        connection.send(header);

        AmqpHeader response = connection.readAmqpHeader();
        assertNotNull(response);
        LOG.info("Broker responded with: {}", response);

        assertTrue("Broker should have closed client connection", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                try {
                    connection.send(header);
                    return false;
                } catch (Exception e) {
                    return true;
                }
            }
        }, TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(250)));
    }

    private ClientConnection createClientConnection(boolean ssl) {
        if (ssl) {
            return new SslClientConnection();
        } else {
            return new ClientConnection();
        }
    }

    private class ClientConnection {

        protected static final long RECEIVE_TIMEOUT = 10000;
        protected Socket clientSocket;

        public void open(String host, int port) throws IOException, UnknownHostException {
            clientSocket = new Socket(host, port);
            clientSocket.setTcpNoDelay(true);
        }

        public void send(AmqpHeader header) throws Exception {
            OutputStream outputStream = clientSocket.getOutputStream();
            header.getBuffer().writeTo(outputStream);
            outputStream.flush();
        }

        public AmqpHeader readAmqpHeader() throws Exception {
            clientSocket.setSoTimeout((int)RECEIVE_TIMEOUT);
            InputStream is = clientSocket.getInputStream();

            byte[] header = new byte[8];
            int read = is.read(header);
            if (read == header.length) {
                return new AmqpHeader(new Buffer(header));
            } else {
                return null;
            }
        }
    }

    private class SslClientConnection extends ClientConnection {

        @Override
        public void open(String host, int port) throws IOException, UnknownHostException {
            SocketFactory factory = SSLSocketFactory.getDefault();
            clientSocket = factory.createSocket(host, port);
            clientSocket.setTcpNoDelay(true);
        }
    }

    @Override
    protected boolean isUseTcpConnector() {
        return true;
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
}
