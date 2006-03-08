/**
 * 
 * Copyright 2005 LogicBlaze, Inc. http://www.logicblaze.com
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
 * 
 **/
package org.apache.activemq.broker.jmx;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

/**
 * 
 * @version $Revision$
 */
public class MBeanTest extends EmbeddedBrokerTestSupport {

    protected MBeanServer mbeanServer;
    protected String domain = "org.apache.activemq";

    protected Connection connection;
    protected boolean transacted;
    protected int authMode = Session.AUTO_ACKNOWLEDGE;
    protected int messageCount = 10;

    public void testDummy() throws Exception {
    }

    public void XXXX_testMBeans() throws Exception {
        connection = connectionFactory.createConnection();
        useConnection(connection);

        // test all the various MBeans now we have a producer, consumer and
        // messages on a queue
        assertQueueBrowseWorks();
    }

    protected void assertQueueBrowseWorks() throws Exception {

        Integer mbeancnt = mbeanServer.getMBeanCount();
        echo("Mbean count :" + mbeancnt);

        ObjectName queueViewMBeanName = new ObjectName(domain + ":Type=Queue,Destination=" + getDestinationString() + ",BrokerName=localhost");

        if (mbeanServer.isRegistered(queueViewMBeanName)) {
            echo("Bean Registered: " + queueViewMBeanName);
        }
        else {
            fail("Could not find MBean!: " + queueViewMBeanName);
        }

        echo("\nCreate QueueView MBean...");

        QueueViewMBean proxy = (QueueViewMBean) MBeanServerInvocationHandler.newProxyInstance(mbeanServer, queueViewMBeanName, QueueViewMBean.class, true);

        long concount = proxy.getConsumerCount();
        echo("Consumer Count :" + concount);
        long messcount = proxy.getQueueSize();
        echo("current number of messages in the queue :" + messcount);

        
        
        // lets browse
        CompositeData[] compdatalist = proxy.browse();
        if (compdatalist.length == 0) {
            fail("There is no message in the queue:");
        }
        else {
            for (int i = 0; i < compdatalist.length; i++) {
                CompositeData cdata = compdatalist[i];
                echo("message " + i + " : " + cdata.toString());
            }
        }

        TabularData table = proxy.browseAsTable();
        echo("Found tabular data: " + table);
        assertTrue("Table should not be empty!", table.size() > 0);
        

        /*
        String messageID = null;
        String newDestinationName = "queue://dummy.test.cheese";
        proxy.copyMessageTo(messageID, newDestinationName);
        proxy.removeMessage(messageID);
        */

    }

    protected void setUp() throws Exception {
        bindAddress = "tcp://localhost:61616";
        useTopic = false;
        super.setUp();
        mbeanServer = broker.getManagementContext().getMBeanServer();
    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
        super.tearDown();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseJmx(true);
        answer.setPersistent(false);
        answer.addConnector(bindAddress);
        return answer;
    }

    protected void useConnection(Connection connection) throws Exception {
        connection.setClientID("foo");
        connection.start();
        Session session = connection.createSession(transacted, authMode);
        destination = createDestination();
        MessageProducer producer = session.createProducer(destination);
        for (int i = 0; i < messageCount; i++) {
            Message message = session.createTextMessage("Message: " + i);
            producer.send(message);
        }
        Thread.sleep(1000);
    }

    protected void echo(String text) {
        System.out.println(text);
    }
}
