/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.transport.peer;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.util.MessageIdList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class PeerTransportTest extends TestCase {
    protected Log log = LogFactory.getLog(getClass());
    protected ActiveMQDestination destination;
    protected boolean topic = true;
    protected static final int MESSAGE_COUNT = 50;
    protected static final int NUMBER_IN_CLUSTER = 3;
    protected int deliveryMode = DeliveryMode.NON_PERSISTENT;
    protected MessageProducer[] producers;
    protected Connection[] connections;
    protected MessageIdList messageIdList[];

    protected void setUp() throws Exception {
        
        connections = new Connection[NUMBER_IN_CLUSTER];
        producers = new MessageProducer[NUMBER_IN_CLUSTER];
        messageIdList = new MessageIdList[NUMBER_IN_CLUSTER];
        ActiveMQDestination destination = createDestination();

        String root = System.getProperty("activemq.store.dir");

        
        for (int i = 0;i < NUMBER_IN_CLUSTER;i++) {
            connections[i] = createConnection(i);
            connections[i].setClientID("ClusterTest" + i);
            connections[i].start();

            Session session = connections[i].createSession(false, Session.AUTO_ACKNOWLEDGE);
            producers[i] = session.createProducer(destination);
            producers[i].setDeliveryMode(deliveryMode);
            MessageConsumer consumer = createMessageConsumer(session, destination);
            messageIdList[i] = new MessageIdList();
            consumer.setMessageListener(messageIdList[i]);
        }
        
        log.info("Waiting for cluster to be fully connected");
        
        // Each connection should see that NUMBER_IN_CLUSTER consumers get registered on the destination.
        ActiveMQDestination advisoryDest = AdvisorySupport.getConsumerAdvisoryTopic(destination);
        for (int i = 0;i < NUMBER_IN_CLUSTER;i++) {
            Session session = connections[i].createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = createMessageConsumer(session, advisoryDest);
            
            int j=0;
            while(j < NUMBER_IN_CLUSTER) {
                ActiveMQMessage message = (ActiveMQMessage) consumer.receive(1000);
                if( message == null ) {
                    fail("Connection "+i+" saw "+j+" consumers, expected: "+NUMBER_IN_CLUSTER);
                }
                if( message.getDataStructure()!=null && message.getDataStructure().getDataStructureType()==ConsumerInfo.DATA_STRUCTURE_TYPE ) {
                    j++;
                }
            }
            
            session.close();
        }
        
        log.info("Cluster is online.");
    }

    protected void tearDown() throws Exception {
        if (connections != null) {
            for (int i = 0;i < connections.length;i++) {
                connections[i].close();
            }
        }
    }

    protected MessageConsumer createMessageConsumer(Session session, Destination destination) throws JMSException {
        return session.createConsumer(destination);
    }

    protected Connection createConnection(int i) throws JMSException {
        log.info("creating connection ....");
        ActiveMQConnectionFactory fac = new ActiveMQConnectionFactory("peer://" + getClass().getName()+"/node"+i);
        return fac.createConnection();
    }

    protected ActiveMQDestination createDestination() {
        return createDestination(getClass().getName());
    }

    protected ActiveMQDestination createDestination(String name) {
        if (topic) {
            return new ActiveMQTopic(name);
        }
        else {
            return new ActiveMQQueue(name);
        }
    }


    /**
     * @throws Exception
     */
    public void testSendReceive() throws Exception {
        for (int i = 0;i < MESSAGE_COUNT;i++) {
            for (int x = 0;x < producers.length;x++) {
                TextMessage textMessage = new ActiveMQTextMessage();
                textMessage.setText("MSG-NO: " + i + " in cluster: " + x);
                producers[x].send(textMessage);
            }
        }
        
        for (int i = 0;i < NUMBER_IN_CLUSTER;i++) {
            messageIdList[i].assertMessagesReceived(expectedReceiveCount());
        }
    }
    
    protected int expectedReceiveCount() {
        return MESSAGE_COUNT * NUMBER_IN_CLUSTER;
    }

}
