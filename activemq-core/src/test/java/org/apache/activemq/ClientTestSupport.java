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
package org.apache.activemq;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.JMSException;

import junit.framework.TestCase;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.transport.TransportFactory;

public class ClientTestSupport extends TestCase {

    protected BrokerService broker;
    protected long idGenerator;

    private ActiveMQConnectionFactory connFactory;
    private String brokerURL = "vm://localhost?broker.persistent=false";

    public void setUp() throws Exception {
        final AtomicBoolean connected = new AtomicBoolean(false);
        TransportConnector connector;

        // Start up a broker with a tcp connector.
        try {
            broker = BrokerFactory.createBroker(new URI(this.brokerURL));
            String brokerId = broker.getBrokerName();
            connector = new TransportConnector(broker.getBroker(), TransportFactory.bind(brokerId, new URI(this.brokerURL))) {
                // Hook into the connector so we can assert that the server
                // accepted a connection.
                protected org.apache.activemq.broker.Connection createConnection(org.apache.activemq.transport.Transport transport) throws IOException {
                    connected.set(true);
                    return super.createConnection(transport);
                }
            };
            connector.start();
            broker.start();

        } catch (IOException e) {
            throw new JMSException("Error creating broker " + e);
        } catch (URISyntaxException e) {
            throw new JMSException("Error creating broker " + e);
        }

        URI connectURI;
        connectURI = connector.getServer().getConnectURI();

        // This should create the connection.
        connFactory = new ActiveMQConnectionFactory(connectURI);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if (broker != null) {
            broker.stop();
        }
    }

    public ActiveMQConnectionFactory getConnectionFactory() throws JMSException {
        if (this.connFactory == null) {
            throw new JMSException("ActiveMQConnectionFactory is null ");
        }
        return this.connFactory;
    }

    // Helper Classes
    protected ConnectionInfo createConnectionInfo() throws Exception {
        ConnectionInfo info = new ConnectionInfo();
        info.setConnectionId(new ConnectionId("connection:" + (++idGenerator)));
        info.setClientId(info.getConnectionId().getValue());
        return info;
    }

    protected SessionInfo createSessionInfo(ConnectionInfo connectionInfo) throws Exception {
        SessionInfo info = new SessionInfo(connectionInfo, ++idGenerator);
        return info;
    }

    protected ConsumerInfo createConsumerInfo(SessionInfo sessionInfo, ActiveMQDestination destination) throws Exception {
        ConsumerInfo info = new ConsumerInfo(sessionInfo, ++idGenerator);
        info.setBrowser(false);
        info.setDestination(destination);
        info.setPrefetchSize(1000);
        info.setDispatchAsync(false);
        return info;
    }

    protected RemoveInfo closeConsumerInfo(ConsumerInfo consumerInfo) {
        return consumerInfo.createRemoveCommand();
    }

    protected MessageAck createAck(ConsumerInfo consumerInfo, Message msg, int count, byte ackType) {
        MessageAck ack = new MessageAck();
        ack.setAckType(ackType);
        ack.setConsumerId(consumerInfo.getConsumerId());
        ack.setDestination(msg.getDestination());
        ack.setLastMessageId(msg.getMessageId());
        ack.setMessageCount(count);
        return ack;
    }

    protected Message receiveMessage(StubConnection connection, int MAX_WAIT) throws InterruptedException {
        while (true) {
            Object o = connection.getDispatchQueue().poll(MAX_WAIT, TimeUnit.MILLISECONDS);

            if (o == null)
                return null;

            if (o instanceof MessageDispatch) {
                MessageDispatch dispatch = (MessageDispatch)o;
                return dispatch.getMessage();
            }
        }
    }

    protected Broker getBroker() throws Exception {
        return this.broker != null ? this.broker.getBroker() : null;
    }

    public static void removeMessageStore() {
        if (System.getProperty("activemq.store.dir") != null) {
            recursiveDelete(new File(System.getProperty("activemq.store.dir")));
        }
        if (System.getProperty("derby.system.home") != null) {
            recursiveDelete(new File(System.getProperty("derby.system.home")));
        }
    }

    public static void recursiveDelete(File f) {
        if (f.isDirectory()) {
            File[] files = f.listFiles();
            for (int i = 0; i < files.length; i++) {
                recursiveDelete(files[i]);
            }
        }
        f.delete();
    }

}
