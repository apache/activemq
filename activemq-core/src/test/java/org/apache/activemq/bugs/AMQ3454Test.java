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
package org.apache.activemq.bugs;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ3454Test extends TestCase {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ3454Test.class);
    private static final int MESSAGES_COUNT = 10000;

    public void testSendWithLotsOfDestinations() throws Exception {
        final BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setDeleteAllMessagesOnStartup(true);

        broker.addConnector("tcp://localhost:0");

        // populate a bunch of destinations, validate the impact on a call to send
        ActiveMQDestination[] destinations = new ActiveMQDestination[MESSAGES_COUNT];
        for (int idx = 0; idx < MESSAGES_COUNT; ++idx) {
            destinations[idx] = new ActiveMQQueue(getDestinationName() + "-" + idx);
        }
        broker.setDestinations(destinations);
        broker.start();

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                broker.getTransportConnectors().get(0).getPublishableConnectString());
        final Connection connection = factory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(new ActiveMQQueue(getDestinationName()));

        long start = System.currentTimeMillis();
        for (int idx = 0; idx < MESSAGES_COUNT; ++idx) {
            Message message = session.createTextMessage("" + idx);
            producer.send(message);
        }
        LOG.info("Duration: " + (System.currentTimeMillis() - start) + " millis");
        producer.close();
        session.close();

    }

    protected String getDestinationName() {
        return getClass().getName() + "." + getName();
    }
}
