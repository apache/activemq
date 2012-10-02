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
package org.apache.activemq.usecases;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.TestSupport;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdvisoryTopicDeletionTest extends TestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(AdvisoryTopicDeletionTest.class);

    private BrokerService broker;
    private Connection connection;

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://" + getName());
    }

    protected void setUp() throws Exception {
        createBroker();
        topic = false;
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        destroyBroker();
    }

    private void createBroker() throws Exception {
        broker = BrokerFactory.createBroker("broker:(vm://localhost)");
        broker.setPersistent(false);
        broker.setBrokerName(getName());
        broker.start();

        connection = createConnection();
    }

    @Override
    protected Connection createConnection() throws Exception {
        Connection con = super.createConnection();
        con.start();
        return con;
    }

    private void destroyBroker() throws Exception {
        if (connection != null)
            connection.close();
        if (broker != null)
            broker.stop();
    }

    public void doTest() throws Exception {
        Destination dest = createDestination();

        Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Session consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer consumer = consumerSession.createConsumer(dest);

        MessageProducer prod = producerSession.createProducer(dest);
        Message message = producerSession.createMessage();
        prod.send(message);

        consumer.receive(60 * 1000);
        connection.close();
        connection = null;

        if ( topic ) {
            broker.getAdminView().removeTopic(((ActiveMQDestination)dest).getPhysicalName());
        } else {
            broker.getAdminView().removeQueue(((ActiveMQDestination)dest).getPhysicalName());
        }

        ActiveMQDestination dests[] = broker.getRegionBroker().getDestinations();
        int matchingDestinations = 0;
        for (ActiveMQDestination destination: dests) {
            String name = destination.getPhysicalName();
            LOG.debug("Found destination " + name);
            if (name.startsWith("ActiveMQ.Advisory") && name.contains(getDestinationString())) {
                matchingDestinations++;
            }
        }

        assertEquals("No matching destinations should be found", 0, matchingDestinations);
    }

    public void testTopic() throws Exception {
        topic=true;
        doTest();
    }

    public void testQueue() throws Exception {
        topic=false;
        doTest();
    }
}
