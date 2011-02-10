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
package org.apache.activemq.perf;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version $Revision: 1.3 $
 */
public class MemoryAllocationTest extends TestCase {

    protected static final int MESSAGE_COUNT = 2000;
    private static final Logger LOG = LoggerFactory.getLogger(MemoryAllocationTest.class);

    protected BrokerService broker;
    protected String bindAddress = "vm://localhost";
    protected int topicCount;

    public void testPerformance() throws Exception {
        ConnectionFactory factory = createConnectionFactory();
        Connection connection = factory.createConnection();
        for (int i = 0; i < MESSAGE_COUNT; i++) {

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination dest = session.createTemporaryTopic();
            session.createConsumer(dest);
            MessageProducer mp = session.createProducer(dest);
            Message msg = session.createTextMessage("test" + i);
            mp.send(msg);
            session.close();
            releaseDestination(dest);
            if (i % 500 == 0) {
                LOG.info("Iterator " + i);
            }
        }
        connection.close();
    }

    protected Destination getDestination(Session session) throws JMSException {
        String topicName = getClass().getName() + "." + topicCount++;
        return session.createTopic(topicName);
    }

    protected void releaseDestination(Destination dest) throws JMSException {
        if (dest instanceof TemporaryTopic) {
            TemporaryTopic tt = (TemporaryTopic)dest;
            tt.delete();
        } else if (dest instanceof TemporaryQueue) {
            TemporaryQueue tq = (TemporaryQueue)dest;
            tq.delete();
        }
    }

    protected void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker();
        }
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();

        if (broker != null) {
            broker.stop();
        }
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(bindAddress);
        return cf;
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        configureBroker(answer);
        answer.start();
        return answer;
    }

    protected void configureBroker(BrokerService answer) throws Exception {
        answer.setPersistent(false);
        answer.addConnector(bindAddress);
        answer.setDeleteAllMessagesOnStartup(true);
    }
}
