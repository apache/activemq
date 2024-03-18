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
package org.apache.activemq.broker.policy;

import java.io.File;
import java.util.Random;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageFormatRuntimeException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.MessageInterceptorStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.activemq.TestSupport;
import org.apache.activemq.util.ByteSequence;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * This unit test is to test that MessageInterceptorStrategy features
 *
 */
public class MessageInterceptorStrategyMemoryUsageTest extends TestSupport {

    BrokerService broker;
    ConnectionFactory factory;
    Connection connection;
    Session session;
    MessageProducer producer;
    QueueBrowser queueBrowser;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();

        File testDataDir = new File("target/activemq-data/message-interceptor-strategy");
        broker.setDataDirectoryFile(testDataDir);
        broker.setUseJmx(true);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.getSystemUsage().getMemoryUsage().setLimit(1024l * 1024 * 64);
        broker.setPersistenceAdapter(new MemoryPersistenceAdapter());
        broker.addConnector("tcp://localhost:0");
        broker.start();
        factory = new ActiveMQConnectionFactory(broker.getTransportConnectors()
                .get(0).getConnectUri().toString());
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @After
    public void tearDown() throws Exception {
        if(producer != null) {
            producer.close();
        }
        session.close();
        connection.stop();
        connection.close();
        broker.stop();
    }

    /**
     * Test sending messages that have body modified have correct usage
     * 
     * Start with 10x 1k message bodies that get increased to 1mb
     */
    @Test
    public void testMemoryUsageBodyIncrease() throws Exception {
        applyHeaderMessageInterceptor(1*1024*1024);
        String queueName = "mis.bodySize.increase";
        Queue queue = createQueue(queueName);

        for (int i=0; i<10; i++) {
            BytesMessage sendMessageP = session.createBytesMessage();
            byte[] origBody = new byte[1*1024];
            sendMessageP.writeBytes(origBody);
            producer.send(queue, sendMessageP);
        }

        QueueViewMBean queueViewMBean = getProxyToQueue(queueName);
        assertEquals(Long.valueOf(10_496_000l), Long.valueOf(queueViewMBean.getMemoryUsageByteCount()));
    }

    /**
     * Test sending messages that have body modified have correct usage
     * 
     * Start with 10x 1mb message bodies that get decreased to 1kb
     */
    @Test
    public void testMemoryUsageBodyDecrease() throws Exception {
        applyHeaderMessageInterceptor(1*1024);
        String queueName = "mis.bodySize.decrease";
        Queue queue = createQueue(queueName);

        for (int i=0; i<10; i++) {
            BytesMessage sendMessageP = session.createBytesMessage();
            byte[] origBody = new byte[1*1024*1024];
            sendMessageP.writeBytes(origBody);
            producer.send(queue, sendMessageP);
        }

        QueueViewMBean queueViewMBean = getProxyToQueue(queueName);
        assertEquals(Long.valueOf(20_480), Long.valueOf(queueViewMBean.getMemoryUsageByteCount()));
    }

    private PolicyMap applyHeaderMessageInterceptor(final int bodySize) {
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();

        MessageInterceptorStrategy bodySizeMessageInterceptorStrategy= new MessageInterceptorStrategy() {

            @Override
            public void process(ProducerBrokerExchange producerBrokerExchange, org.apache.activemq.command.Message message) throws MessageFormatRuntimeException {
                if(bodySize > 0) {
                    try {
                        message.clearBody();
                    } catch (JMSException e) {
                        fail(e.getMessage());
                    }
                    byte[] newBody = new byte[bodySize];
                    new Random().nextBytes(newBody);
                    message.setContent(new ByteSequence(newBody));
                    message.storeContent();
                }
            }
        };
        defaultEntry.setMessageInterceptorStrategy(bodySizeMessageInterceptorStrategy);

        policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);
        return policyMap;
    }

    private Queue createQueue(String queueName) throws Exception {
        Queue queue = session.createQueue(queueName);
        producer = session.createProducer(queue);
        return queue;
    }

}
