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

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Topic;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.test.JmsTopicSendReceiveTest;


/**
 * @version $Revision: 1.3 $
 */
public class JmsQueueCompositeSendReceiveTest extends JmsTopicSendReceiveTest {
    private static final org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
            .getLog(JmsQueueCompositeSendReceiveTest.class);
    
    /**
     * Sets a test to have a queue destination and non-persistent delivery mode.
     *
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        topic = false;
        deliveryMode = DeliveryMode.NON_PERSISTENT;
        super.setUp();
    }

    /**
     * Returns the consumer subject.
     *
     * @return String - consumer subject
     * @see org.apache.activemq.test.TestSupport#getConsumerSubject()
     */
    protected String getConsumerSubject() {
        return "FOO.BAR.HUMBUG";
    }

    /**
     * Returns the producer subject.
     *
     * @return String - producer subject
     * @see org.apache.activemq.test.TestSupport#getProducerSubject()
     */
    protected String getProducerSubject() {
        return "FOO.BAR.HUMBUG,FOO.BAR.HUMBUG2";
    }
   
    /**
     * Test if all the messages sent are being received.
     *
     * @throws Exception
     */
    public void testSendReceive() throws Exception {
        super.testSendReceive();
        messages.clear();
        Destination consumerDestination = consumeSession.createQueue("FOO.BAR.HUMBUG2");
        LOG.info("Created  consumer destination: " + consumerDestination + " of type: " + consumerDestination.getClass());
        MessageConsumer consumer = null;
        if (durable) {
            LOG.info("Creating durable consumer");
            consumer = consumeSession.createDurableSubscriber((Topic) consumerDestination, getName());
        } else {
            consumer = consumeSession.createConsumer(consumerDestination);
        }
        consumer.setMessageListener(this);

        assertMessagesAreReceived();
        LOG.info("" + data.length + " messages(s) received, closing down connections");
    }
    
    public void testDuplicate() throws Exception {
    	ActiveMQDestination queue = (ActiveMQDestination)session.createQueue("TEST,TEST");
        for (int i = 0; i < data.length; i++) {
            Message message = createMessage(i);
            configureMessage(message);
            if (verbose) {
                LOG.info("About to send a message: " + message + " with text: " + data[i]);
            }
            producer.send(queue, message);
        }
        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:1099/jmxrmi");
        JMXConnector connector = JMXConnectorFactory.connect(url, null);
        connector.connect();
        MBeanServerConnection connection = connector.getMBeanServerConnection();
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:Type=Queue,Destination=TEST,BrokerName=localhost");
        
        QueueViewMBean queueMbean = (QueueViewMBean) MBeanServerInvocationHandler.newProxyInstance(connection, queueViewMBeanName, QueueViewMBean.class, true);
        assertEquals(data.length, queueMbean.getQueueSize());
        queueMbean.purge();
        assertEquals(0, queueMbean.getQueueSize());
    }
}
