/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.activemq.transport;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicInteger;

import org.activemq.ActiveMQConnectionFactory;
import org.activemq.broker.BrokerService;
import org.activemq.broker.TransportConnector;
import org.activemq.command.ActiveMQQueue;
import org.activemq.command.ActiveMQTextMessage;
import org.activemq.command.ActiveMQTopic;
import org.activemq.network.NetworkConnector;
import org.activemq.transport.discovery.rendezvous.RendezvousDiscoveryAgent;
import org.activemq.util.ServiceStopper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class TopicClusterTest extends TestCase implements MessageListener {
    protected Log log = LogFactory.getLog(getClass());
    protected Destination destination;
    protected boolean topic = true;
    protected AtomicInteger receivedMessageCount = new AtomicInteger(0);
    protected static int MESSAGE_COUNT = 50;
    protected static int NUMBER_IN_CLUSTER = 3;
    protected int deliveryMode = DeliveryMode.NON_PERSISTENT;
    protected MessageProducer[] producers;
    protected Connection[] connections;
    protected List services = new ArrayList();

    protected void setUp() throws Exception {
        connections = new Connection[NUMBER_IN_CLUSTER];
        producers = new MessageProducer[NUMBER_IN_CLUSTER];
        Destination destination = createDestination();
        int portStart = 50000;
        String root = System.getProperty("activemq.store.dir");
        if (root == null) {
            root = "target/store";
        }
        try {
            for (int i = 0;i < NUMBER_IN_CLUSTER;i++) {

                System.setProperty("activemq.store.dir", root + "_broker_" + i);
                connections[i] = createConnection("broker-" + i);
                connections[i].setClientID("ClusterTest" + i);
                connections[i].start();
                Session session = connections[i].createSession(false, Session.AUTO_ACKNOWLEDGE);
                producers[i] = session.createProducer(destination);
                producers[i].setDeliveryMode(deliveryMode);
                MessageConsumer consumer = createMessageConsumer(session,destination);
                consumer.setMessageListener(this);

            }
            System.out.println("Sleeping to ensure cluster is fully connected");
            Thread.sleep(5000);
        } finally {
            System.setProperty("activemq.store.dir", root);
        }
    }

    protected void tearDown() throws Exception {
        if (connections != null) {
            for (int i = 0;i < connections.length;i++) {
                connections[i].close();
            }
        }
        ServiceStopper stopper = new ServiceStopper();
        stopper.stopServices(services);
    }
    
    protected MessageConsumer createMessageConsumer(Session session, Destination destination) throws JMSException{
        return session.createConsumer(destination);
    }

    protected ActiveMQConnectionFactory createGenericClusterFactory(String brokerName) throws Exception {
        BrokerService container = new BrokerService();
        container.setBrokerName(brokerName);
      
        String url = "tcp://localhost:0";
        TransportConnector connector = container.addConnector(url);
        connector.setDiscoveryUri(new URI("multicast://default"));        
        container.addNetworkConnector("multicast://default");
        container.start();
        
        services.add(container);
        
        return new ActiveMQConnectionFactory("vm://"+brokerName);
    }

    protected int expectedReceiveCount() {
        return MESSAGE_COUNT * NUMBER_IN_CLUSTER * NUMBER_IN_CLUSTER;
    }

    protected Connection createConnection(String name) throws Exception {
        return createGenericClusterFactory(name).createConnection();
    }

    protected Destination createDestination() {
        return createDestination(getClass().getName());
    }

    protected Destination createDestination(String name) {
        if (topic) {
            return new ActiveMQTopic(name);
        }
        else {
            return new ActiveMQQueue(name);
        }
    }


    /**
     * @param msg
     */
    public void onMessage(Message msg) {
        //System.out.println("GOT: " + msg);
        receivedMessageCount.incrementAndGet();
        synchronized (receivedMessageCount) {
            if (receivedMessageCount.get() >= expectedReceiveCount()) {
                receivedMessageCount.notify();
            }
        }
    }

    /**
     * @throws Exception
     */
    public void testSendReceive() throws Exception {
        for (int i = 0;i < MESSAGE_COUNT;i++) {
            TextMessage textMessage = new ActiveMQTextMessage();
            textMessage.setText("MSG-NO:" + i);
            for (int x = 0;x < producers.length;x++) {
                producers[x].send(textMessage);
            }
        }
        synchronized (receivedMessageCount) {
            if (receivedMessageCount.get() < expectedReceiveCount()) {
                receivedMessageCount.wait(20000);
            }
        }
        //sleep a little - to check we don't get too many messages
        Thread.sleep(2000);
        System.err.println("GOT: " + receivedMessageCount.get());
        assertEquals("Expected message count not correct", expectedReceiveCount(), receivedMessageCount.get());
    }

}