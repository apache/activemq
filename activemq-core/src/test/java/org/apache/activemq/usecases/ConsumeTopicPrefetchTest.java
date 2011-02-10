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
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.policy.IndividualDeadLetterViaXmlTest;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class ConsumeTopicPrefetchTest extends ProducerConsumerTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumeTopicPrefetchTest.class);

    protected int prefetchSize = 100;
    protected String[] messageTexts;
    protected long consumerTimeout = 10000L;

    public void testSendPrefetchSize() throws JMSException {
        testWithMessageCount(prefetchSize);
    }

    public void testSendDoublePrefetchSize() throws JMSException {
        testWithMessageCount(prefetchSize * 2);
    }

    public void testSendPrefetchSizePlusOne() throws JMSException {
        testWithMessageCount(prefetchSize + 1);
    }

    protected void testWithMessageCount(int messageCount) throws JMSException {
        makeMessages(messageCount);

        LOG.info("About to send and receive: " + messageCount + " on destination: " + destination
                + " of type: " + destination.getClass().getName());

        for (int i = 0; i < messageCount; i++) {
            Message message = session.createTextMessage(messageTexts[i]);
            producer.send(message);
        }

        validateConsumerPrefetch(this.getSubject(), prefetchSize);
        
        // lets consume them in two fetch batches
        for (int i = 0; i < messageCount; i++) {
            consumeMessge(i);
        }
    }

    protected Connection createConnection() throws Exception {
        ActiveMQConnection connection = (ActiveMQConnection) super.createConnection();
        connection.getPrefetchPolicy().setQueuePrefetch(prefetchSize);
        connection.getPrefetchPolicy().setTopicPrefetch(prefetchSize);
        return connection;
    }

    protected TextMessage consumeMessge(int i) throws JMSException {
        Message message = consumer.receive(consumerTimeout);
        assertTrue("Should have received a message by now for message: " + i, message != null);
        assertTrue("Should be a TextMessage: " + message, message instanceof TextMessage);
        TextMessage textMessage = (TextMessage) message;
        assertEquals("Message content", messageTexts[i], textMessage.getText());
        return textMessage;
    }


    protected void makeMessages(int messageCount) {
        messageTexts = new String[messageCount];
        for (int i = 0; i < messageCount; i++) {
            messageTexts[i] = "Message for test: + " + getName() + " = " + i;
        }
    }

    protected void validateConsumerPrefetch(String destination, final long expectedCount) throws JMSException {
        RegionBroker regionBroker = (RegionBroker) BrokerRegistry.getInstance().lookup("localhost").getRegionBroker();
        for (org.apache.activemq.broker.region.Destination dest : regionBroker.getQueueRegion().getDestinationMap().values()) {
            final org.apache.activemq.broker.region.Destination target = dest;
            if (dest.getName().equals(destination)) {
                try {
                    Wait.waitFor(new Condition() {
                        public boolean isSatisified() throws Exception {
                            DestinationStatistics stats = target.getDestinationStatistics();
                            LOG.info("inflight for : " + target.getName() + ": " +  stats.getInflight().getCount());
                            return stats.getInflight().getCount() == expectedCount;
                        }
                    });
                } catch (Exception e) {
                    throw new JMSException(e.toString());
                }
                DestinationStatistics stats = dest.getDestinationStatistics();
                LOG.info("inflight for : " + dest.getName() + ": " + stats.getInflight().getCount());
                assertEquals("inflight for: " + dest.getName() + ": " + stats.getInflight().getCount() + " matches", 
                        expectedCount, stats.getInflight().getCount());      
            }
        }
    }
}
