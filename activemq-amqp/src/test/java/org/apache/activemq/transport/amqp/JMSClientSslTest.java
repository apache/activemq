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
package org.apache.activemq.transport.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.amqp.joram.ActiveMQAdmin;
import org.apache.activemq.util.NioSslTestUtil;
import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.provider.amqp.AmqpProvider;
import org.apache.qpid.jms.transports.netty.NettyTcpTransport;
import org.objectweb.jtests.jms.framework.TestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the JMS client when connected to the SSL transport.
 */
public class JMSClientSslTest extends JMSClientTest {
    protected static final Logger LOG = LoggerFactory.getLogger(JMSClientSslTest.class);

    protected String enabledProtocols = null;


    @Override
    public void setUp() throws Exception {
        enabledProtocols = null;
        super.setUp();
    }


    @Override
    protected URI getBrokerURI() {
        return amqpSslURI;
    }

    @Override
    protected boolean isUseTcpConnector() {
        return false;
    }

    @Override
    protected boolean isUseSslConnector() {
        return true;
    }

    @Override
    protected String getTargetConnectorName() {
        return "amqp+ssl";
    }

    protected void testSslHandshakeRenegotiation(String protocol) throws Exception {
        enabledProtocols = protocol;

        ActiveMQAdmin.enableJMSFrameTracing();

        connection = createConnection();

        JmsConnection jmsCon = (JmsConnection) connection;
        NettyTcpTransport transport = getNettyTransport(jmsCon);
        Channel channel = getNettyChannel(transport);
        SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
        assertEquals(protocol, sslHandler.engine().getSession().getProtocol());

        // trigger handshakes
        for (int i = 0; i < 10; i++) {
            sslHandler.engine().beginHandshake();
        }

        // give some time for the handshake updates
        Thread.sleep(100);

        // check status advances if NIOSSL, then continue
        // below to verify transports are not stuck
        checkHandshakeStatusAdvances(((InetSocketAddress)channel.localAddress()).getPort());

        // Make sure messages still work
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getDestinationName());
        MessageProducer p = session.createProducer(queue);

        TextMessage message = session.createTextMessage();
        message.setText("hello");
        p.send(message);

        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(100);
        assertNotNull(msg);
        assertTrue(msg instanceof TextMessage);

    }

    // This only applies to NIO SSL
    protected void checkHandshakeStatusAdvances(int localPort) throws Exception {
        TransportConnector connector = brokerService.getTransportConnectorByScheme(getBrokerURI().getScheme());
        NioSslTestUtil.checkHandshakeStatusAdvances(connector, localPort);
    }

    private NettyTcpTransport getNettyTransport(JmsConnection jmsCon) throws Exception {
        Field providerField = JmsConnection.class.getDeclaredField("provider");
        providerField.setAccessible(true);
        AmqpProvider provider = (AmqpProvider) providerField.get(jmsCon);
        return (NettyTcpTransport) provider.getTransport();
    }

    private Channel getNettyChannel(NettyTcpTransport transport) throws Exception {
        Field channelField = NettyTcpTransport.class.getDeclaredField("channel");
        channelField.setAccessible(true);
        return (Channel) channelField.get(transport);
    }

}
