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
package org.apache.activemq.broker.jmx;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.HashMap;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import junit.textui.TestRunner;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.BaseDestination;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A test case of the various MBeans in ActiveMQ. If you want to look at the
 * various MBeans after the test has been run then run this test case as a
 * command line application.
 * 
 * @version $Revision$
 */
public class MBeanTest extends EmbeddedBrokerTestSupport {
    private static final Log LOG = LogFactory.getLog(MBeanTest.class);

    private static boolean waitForKeyPress;

    protected MBeanServer mbeanServer;
    protected String domain = "org.apache.activemq";
    protected String clientID = "foo";

    protected Connection connection;
    protected boolean transacted;
    protected int authMode = Session.AUTO_ACKNOWLEDGE;
    protected static final int MESSAGE_COUNT = 2*BaseDestination.MAX_PAGE_SIZE;

    /**
     * When you run this test case from the command line it will pause before
     * terminating so that you can look at the MBeans state for debugging
     * purposes.
     */
    public static void main(String[] args) {
        waitForKeyPress = true;
        TestRunner.run(MBeanTest.class);
    }

    public void testMBeans() throws Exception {
        connection = connectionFactory.createConnection();
        useConnection(connection);

        // test all the various MBeans now we have a producer, consumer and
        // messages on a queue
        assertSendViaMBean();
        assertQueueBrowseWorks();
        assertCreateAndDestroyDurableSubscriptions();
        assertConsumerCounts();
    }

    public void testMoveMessagesBySelector() throws Exception {
        connection = connectionFactory.createConnection();
        useConnection(connection);

        ObjectName queueViewMBeanName = assertRegisteredObjectName(domain + ":Type=Queue,Destination=" + getDestinationString() + ",BrokerName=localhost");

        QueueViewMBean queue = (QueueViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, queueViewMBeanName, QueueViewMBean.class, true);

        String newDestination = "test.new.destination." + getClass() + "." + getName();
        queue.moveMatchingMessagesTo("counter > 2", newDestination);

        queueViewMBeanName = assertRegisteredObjectName(domain + ":Type=Queue,Destination=" + newDestination + ",BrokerName=localhost");

        queue = (QueueViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, queueViewMBeanName, QueueViewMBean.class, true);
        int movedSize = MESSAGE_COUNT-3;
        assertEquals("Unexpected number of messages ",movedSize,queue.getQueueSize());

        // now lets remove them by selector
        queue.removeMatchingMessages("counter > 2");

        assertEquals("Should have no more messages in the queue: " + queueViewMBeanName, 0, queue.getQueueSize());
    }

    public void testCopyMessagesBySelector() throws Exception {
        connection = connectionFactory.createConnection();
        useConnection(connection);

        ObjectName queueViewMBeanName = assertRegisteredObjectName(domain + ":Type=Queue,Destination=" + getDestinationString() + ",BrokerName=localhost");

        QueueViewMBean queue = (QueueViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, queueViewMBeanName, QueueViewMBean.class, true);

        String newDestination = "test.new.destination." + getClass() + "." + getName();
        long queueSize = queue.getQueueSize();
        queue.copyMatchingMessagesTo("counter > 2", newDestination);

        

        queueViewMBeanName = assertRegisteredObjectName(domain + ":Type=Queue,Destination=" + newDestination + ",BrokerName=localhost");

        queue = (QueueViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, queueViewMBeanName, QueueViewMBean.class, true);

        LOG.info("Queue: " + queueViewMBeanName + " now has: " + queue.getQueueSize() + " message(s)");
        assertEquals("Expected messages in a queue: " + queueViewMBeanName, MESSAGE_COUNT-3, queue.getQueueSize());
        // now lets remove them by selector
        queue.removeMatchingMessages("counter > 2");

        assertEquals("Should have no more messages in the queue: " + queueViewMBeanName, 0, queue.getQueueSize());
    }


    protected void assertSendViaMBean() throws Exception {
        String queueName = getDestinationString() + ".SendMBBean";

        ObjectName brokerName = assertRegisteredObjectName(domain + ":Type=Broker,BrokerName=localhost");
        echo("Create QueueView MBean...");
        BrokerViewMBean broker = (BrokerViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, brokerName, BrokerViewMBean.class, true);
        broker.addQueue(queueName);

        ObjectName queueViewMBeanName = assertRegisteredObjectName(domain + ":Type=Queue,Destination=" + queueName + ",BrokerName=localhost");

        echo("Create QueueView MBean...");
        QueueViewMBean proxy = (QueueViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, queueViewMBeanName, QueueViewMBean.class, true);

        int count = 5;
        for (int i = 0; i < count; i++) {
            String body = "message:" + i;

            Map headers = new HashMap();
            headers.put("JMSCorrelationID", "MyCorrId");
            headers.put("JMSDeliveryMode", Boolean.TRUE);
            headers.put("JMSXGroupID", "MyGroupID");
            headers.put("JMSXGroupSeq", 1234);
            headers.put("JMSPriority", i);
            headers.put("JMSType", "MyType");
            headers.put("MyHeader", i);
            headers.put("MyStringHeader", "StringHeader" + i);

            proxy.sendTextMessage(headers, body);
        }
        
        CompositeData[] compdatalist = proxy.browse();
        if (compdatalist.length == 0) {
            fail("There is no message in the queue:");
        }
        String[] messageIDs = new String[compdatalist.length];

        for (int i = 0; i < compdatalist.length; i++) {
            CompositeData cdata = compdatalist[i];

            if (i == 0) {
                echo("Columns: " + cdata.getCompositeType().keySet());
            }

            assertComplexData(cdata, "JMSCorrelationID", "MyCorrId");
            assertComplexData(cdata, "JMSPriority", i);
            assertComplexData(cdata, "JMSType", "MyType");
            assertComplexData(cdata, "JMSCorrelationID", "MyCorrId");
            assertComplexData(cdata, "PropertiesText", "{MyStringHeader=StringHeader" + i + ", MyHeader=" + i + "}");

            Map intProperties = CompositeDataHelper.getTabularMap(cdata, CompositeDataConstants.INT_PROPERTIES);
            assertEquals("intProperties size()", 1, intProperties.size());
            assertEquals("intProperties.MyHeader", i, intProperties.get("MyHeader"));

            Map stringProperties = CompositeDataHelper.getTabularMap(cdata, CompositeDataConstants.STRING_PROPERTIES);
            assertEquals("stringProperties size()", 1, stringProperties.size());
            assertEquals("stringProperties.MyHeader", "StringHeader" + i, stringProperties.get("MyStringHeader"));

            assertComplexData(cdata, "JMSXGroupSeq", 1234);
            assertComplexData(cdata, "JMSXGroupID", "MyGroupID");
            assertComplexData(cdata, "Text", "message:" + i);

        }
    }

    protected void assertComplexData(CompositeData cdata, String name, Object expected) {
        Object value = cdata.get(name);
        assertEquals("CData field: " + name, expected, value);
    }


    protected void assertQueueBrowseWorks() throws Exception {
        Integer mbeancnt = mbeanServer.getMBeanCount();
        echo("Mbean count :" + mbeancnt);

        ObjectName queueViewMBeanName = assertRegisteredObjectName(domain + ":Type=Queue,Destination=" + getDestinationString() + ",BrokerName=localhost");

        echo("Create QueueView MBean...");
        QueueViewMBean proxy = (QueueViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, queueViewMBeanName, QueueViewMBean.class, true);

        long concount = proxy.getConsumerCount();
        echo("Consumer Count :" + concount);
        long messcount = proxy.getQueueSize();
        echo("current number of messages in the queue :" + messcount);

        // lets browse
        CompositeData[] compdatalist = proxy.browse();
        if (compdatalist.length == 0) {
            fail("There is no message in the queue:");
        }
        String[] messageIDs = new String[compdatalist.length];

        for (int i = 0; i < compdatalist.length; i++) {
            CompositeData cdata = compdatalist[i];

            if (i == 0) {
                echo("Columns: " + cdata.getCompositeType().keySet());
            }
            messageIDs[i] = (String)cdata.get("JMSMessageID");
            echo("message " + i + " : " + cdata.values());
        }

        TabularData table = proxy.browseAsTable();
        echo("Found tabular data: " + table);
        assertTrue("Table should not be empty!", table.size() > 0);

        assertEquals("Queue size", MESSAGE_COUNT, proxy.getQueueSize());

        String messageID = messageIDs[0];
        String newDestinationName = "queue://dummy.test.cheese";
        echo("Attempting to copy: " + messageID + " to destination: " + newDestinationName);
        proxy.copyMessageTo(messageID, newDestinationName);

        assertEquals("Queue size", MESSAGE_COUNT, proxy.getQueueSize());

        messageID = messageIDs[1];
        echo("Attempting to remove: " + messageID);
        proxy.removeMessage(messageID);

        assertEquals("Queue size", MESSAGE_COUNT-1, proxy.getQueueSize());

        echo("Worked!");
    }

    protected void assertCreateAndDestroyDurableSubscriptions() throws Exception {
        // lets create a new topic
        ObjectName brokerName = assertRegisteredObjectName(domain + ":Type=Broker,BrokerName=localhost");
        echo("Create QueueView MBean...");
        BrokerViewMBean broker = (BrokerViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, brokerName, BrokerViewMBean.class, true);

        broker.addTopic(getDestinationString());

        assertEquals("Durable subscriber count", 0, broker.getDurableTopicSubscribers().length);

        String topicName = getDestinationString();
        String selector = null;
        ObjectName name1 = broker.createDurableSubscriber(clientID, "subscriber1", topicName, selector);
        broker.createDurableSubscriber(clientID, "subscriber2", topicName, selector);
        assertEquals("Durable subscriber count", 2, broker.getDurableTopicSubscribers().length);

        assertNotNull("Should have created an mbean name for the durable subscriber!", name1);

        LOG.info("Created durable subscriber with name: " + name1);

        // now lets try destroy it
        broker.destroyDurableSubscriber(clientID, "subscriber1");
        assertEquals("Durable subscriber count", 1, broker.getDurableTopicSubscribers().length);
    }

    protected void assertConsumerCounts() throws Exception {
        ObjectName brokerName = assertRegisteredObjectName(domain + ":Type=Broker,BrokerName=localhost");
        BrokerViewMBean broker = (BrokerViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, brokerName, BrokerViewMBean.class, true);

        // create 2 topics
        broker.addTopic(getDestinationString() + "1");
        broker.addTopic(getDestinationString() + "2");

        ObjectName topicObjName1 = assertRegisteredObjectName(domain + ":Type=Topic,BrokerName=localhost,Destination=" + getDestinationString() + "1");
        ObjectName topicObjName2 = assertRegisteredObjectName(domain + ":Type=Topic,BrokerName=localhost,Destination=" + getDestinationString() + "2");
        TopicViewMBean topic1 = (TopicViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, topicObjName1, TopicViewMBean.class, true);
        TopicViewMBean topic2 = (TopicViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, topicObjName2, TopicViewMBean.class, true);

        assertEquals("topic1 Durable subscriber count", 0, topic1.getConsumerCount());
        assertEquals("topic2 Durable subscriber count", 0, topic2.getConsumerCount());

        String topicName = getDestinationString();
        String selector = null;

        // create 1 subscriber for each topic
        broker.createDurableSubscriber(clientID, "topic1.subscriber1", topicName + "1", selector);
        broker.createDurableSubscriber(clientID, "topic2.subscriber1", topicName + "2", selector);

        assertEquals("topic1 Durable subscriber count", 1, topic1.getConsumerCount());
        assertEquals("topic2 Durable subscriber count", 1, topic2.getConsumerCount());

        // create 1 more subscriber for topic1
        broker.createDurableSubscriber(clientID, "topic1.subscriber2", topicName + "1", selector);

        assertEquals("topic1 Durable subscriber count", 2, topic1.getConsumerCount());
        assertEquals("topic2 Durable subscriber count", 1, topic2.getConsumerCount());

        // destroy topic1 subscriber
        broker.destroyDurableSubscriber(clientID, "topic1.subscriber1");

        assertEquals("topic1 Durable subscriber count", 1, topic1.getConsumerCount());
        assertEquals("topic2 Durable subscriber count", 1, topic2.getConsumerCount());

        // destroy topic2 subscriber
        broker.destroyDurableSubscriber(clientID, "topic2.subscriber1");

        assertEquals("topic1 Durable subscriber count", 1, topic1.getConsumerCount());
        assertEquals("topic2 Durable subscriber count", 0, topic2.getConsumerCount());

        // destroy remaining topic1 subscriber
        broker.destroyDurableSubscriber(clientID, "topic1.subscriber2");

        assertEquals("topic1 Durable subscriber count", 0, topic1.getConsumerCount());
        assertEquals("topic2 Durable subscriber count", 0, topic2.getConsumerCount());
    }

    protected ObjectName assertRegisteredObjectName(String name) throws MalformedObjectNameException, NullPointerException {
        ObjectName objectName = new ObjectName(name);
        if (mbeanServer.isRegistered(objectName)) {
            echo("Bean Registered: " + objectName);
        } else {
            fail("Could not find MBean!: " + objectName);
        }
        return objectName;
    }

    protected void setUp() throws Exception {
        bindAddress = "tcp://localhost:61616";
        useTopic = false;
        super.setUp();
        mbeanServer = broker.getManagementContext().getMBeanServer();
    }

    protected void tearDown() throws Exception {
        if (waitForKeyPress) {
            // We are running from the command line so let folks browse the
            // mbeans...
            System.out.println();
            System.out.println("Press enter to terminate the program.");
            System.out.println("In the meantime you can use your JMX console to view the current MBeans");
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            reader.readLine();
        }

        if (connection != null) {
            connection.close();
            connection = null;
        }
        super.tearDown();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setDeleteAllMessagesOnStartup(true);
        answer.setUseJmx(true);
        //answer.setEnableStatistics(true);
        answer.setPersistent(false);
        answer.addConnector(bindAddress);
        return answer;
    }

    protected void useConnection(Connection connection) throws Exception {
        connection.setClientID(clientID);
        connection.start();
        Session session = connection.createSession(transacted, authMode);
        destination = createDestination();
        MessageProducer producer = session.createProducer(destination);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message message = session.createTextMessage("Message: " + i);
            message.setIntProperty("counter", i);
            message.setJMSCorrelationID("MyCorrelationID");
            message.setJMSReplyTo(new ActiveMQQueue("MyReplyTo"));
            message.setJMSType("MyType");
            message.setJMSPriority(5);
            producer.send(message);
        }
        Thread.sleep(1000);
    }

    protected void echo(String text) {
        LOG.info(text);
    }
}
