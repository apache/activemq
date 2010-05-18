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
import java.net.SocketException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import junit.framework.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.activemq.util.SocketProxy;
import org.apache.activemq.util.URISupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SoWriteTimeoutTest extends JmsTestSupport {
    private static final Log LOG = LogFactory.getLog(SoWriteTimeoutTest.class);
    
    final int receiveBufferSize = 16*1024;
    public String brokerTransportScheme = "nio";
    
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();
        broker.addConnector(brokerTransportScheme + "://localhost:0?transport.soWriteTimeout=1000&transport.sleep=1000&socketBufferSize="+ receiveBufferSize);
        if ("nio".equals(brokerTransportScheme)) {
            broker.addConnector("stomp+" + brokerTransportScheme + "://localhost:0?transport.soWriteTimeout=1000&transport.sleep=1000&socketBufferSize=" + receiveBufferSize + "&trace=true");
        }
        return broker;
    }
    
    public void initCombosForTestWriteTimeout() {
        addCombinationValues("brokerTransportScheme", new Object[]{"tcp", "nio"});
    }
    
    public void testWriteTimeout() throws Exception {
        
        Destination dest = new ActiveMQQueue("testWriteTimeout");
        messageTextPrefix = initMessagePrefix(8*1024);
        sendMessages(dest, 500);
        
        URI tcpBrokerUri = URISupport.removeQuery(broker.getTransportConnectors().get(0).getConnectUri());
        LOG.info("consuming using uri: " + tcpBrokerUri);
        
        SocketProxy proxy = new SocketProxy();
        proxy.setTarget(tcpBrokerUri);
        proxy.setReceiveBufferSize(receiveBufferSize);
        proxy.open();
        
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(proxy.getUrl());
        Connection c = factory.createConnection();
        c.start();
        Session session = c.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer consumer = session.createConsumer(dest); 
        proxy.pause();
        // writes should back up... writeTimeout will kick in a abort the connection
        TimeUnit.SECONDS.sleep(4);
        proxy.goOn();
        assertNotNull("can receive buffered messages", consumer.receive(500));
        try {
            session.commit();
            fail("expect commit to fail as server has aborted writeTimeout connection");
        } catch (JMSException expected) {
        }
    }
    
    public void testWriteTimeoutStompNio() throws Exception {
        ActiveMQQueue dest = new ActiveMQQueue("testWriteTimeout");
        messageTextPrefix = initMessagePrefix(8*1024);
        sendMessages(dest, 500);
        
        URI stompBrokerUri = URISupport.removeQuery(broker.getTransportConnectors().get(1).getConnectUri());
        LOG.info("consuming using uri: " + stompBrokerUri);
        
        SocketProxy proxy = new SocketProxy();
        proxy.setTarget(new URI("tcp://localhost:" + stompBrokerUri.getPort()));
        proxy.setReceiveBufferSize(receiveBufferSize);
        proxy.open();
        
        StompConnection stompConnection = new StompConnection();
        stompConnection.open(new Socket("localhost", proxy.getUrl().getPort()));
        
        String frame = "CONNECT\n" + "login: system\n" + "passcode: manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SUBSCRIBE\n" + "destination:/queue/" + dest.getQueueName() + "\n" + "ack:client\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
        
        // ensure dispatch has started before pause
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));
        
        proxy.pause();
        
        // writes should back up... writeTimeout will kick in a abort the connection
        TimeUnit.SECONDS.sleep(1);

        // see the blocked threads
        //dumpAllThreads("blocked on write");
        
        // abort should be done after this
        TimeUnit.SECONDS.sleep(4);

        proxy.goOn();
        
        // get a buffered message
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));
        
        // verify connection is dead
        try {
            for (int i=0; i<10; i++) {
                stompConnection.send("/queue/" + dest.getPhysicalName(), messageTextPrefix + "ReplyText" + i);
            }
            fail("expected send to fail with timeout out connection");
        } catch (SocketException expected) {
            LOG.info("got exception on send after timeout: " + expected);
        }
    }
    
    private String initMessagePrefix(int i) {
        byte[] content = new byte[i];
        return new String(content);
    }

    public static Test suite() {
        return suite(SoWriteTimeoutTest.class);
    }
}
