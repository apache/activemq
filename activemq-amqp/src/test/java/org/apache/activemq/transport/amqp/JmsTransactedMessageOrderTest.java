/*
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class JmsTransactedMessageOrderTest extends JMSClientTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsTransactedMessageOrderTest.class);

    private final int prefetch;

    public JmsTransactedMessageOrderTest(int prefetch) {
        this.prefetch = prefetch;
    }

    @Parameters(name="Prefetch->{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { {0}, {1}, {100} });
    }

    @Override
    protected void performAdditionalConfiguration(BrokerService brokerService) throws Exception {
        final PolicyMap policyMap = new PolicyMap();
        final List<PolicyEntry> policyEntries = new ArrayList<PolicyEntry>();
        final PolicyEntry policyEntry = new PolicyEntry();

        policyEntry.setQueue(">");
        policyEntry.setStrictOrderDispatch(true);

        policyEntries.add(policyEntry);

        policyMap.setPolicyEntries(policyEntries);
        policyMap.setDefaultEntry(policyEntry);

        brokerService.setDestinationPolicy(policyMap);
    }

    @Test
    public void testMessageOrderAfterRollback() throws Exception {
        sendMessages(5);

        int counter = 0;
        while (counter++ < 20) {
            LOG.info("Creating connection using prefetch of: {}", prefetch);

            JmsConnectionFactory cf = new JmsConnectionFactory(getAmqpURI("jms.prefetchPolicy.all=" + prefetch));

            connection = cf.createConnection();
            connection.start();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue(getDestinationName());
            MessageConsumer consumer = session.createConsumer(queue);

            Message message = consumer.receive(5000);
            assertNotNull(message);
            assertTrue(message instanceof TextMessage);
            LOG.info("Read message = {}", ((TextMessage) message).getText());

            int sequenceID = message.getIntProperty("sequenceID");
            assertEquals(0, sequenceID);

            session.rollback();
            session.close();
            connection.close();
        }
    }

    public void sendMessages(int messageCount) throws JMSException {
        Connection connection = null;
        try {
            connection = createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(getDestinationName());

            for (int i = 0; i < messageCount; ++i) {
                MessageProducer messageProducer = session.createProducer(queue);
                TextMessage message = session.createTextMessage("(" + i + ")");
                message.setIntProperty("sequenceID", i);
                messageProducer.send(message);
                LOG.info("Sent message = {}", message.getText());
            }

        } catch (Exception exp) {
            exp.printStackTrace(System.out);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
