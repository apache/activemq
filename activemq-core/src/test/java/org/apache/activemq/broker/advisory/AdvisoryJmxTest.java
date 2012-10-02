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
package org.apache.activemq.broker.advisory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.DestinationInfo;

import javax.jms.*;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class AdvisoryJmxTest extends EmbeddedBrokerTestSupport {

        protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setPersistent(isPersistent());
        answer.addConnector(bindAddress);
        ManagementContext context = new ManagementContext();
        context.setConnectorPort(1199);
        answer.setManagementContext(context);
        return answer;
    }

    public void testCreateDeleteDestinations() throws Exception {
        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:1199/jmxrmi");
        JMXConnector connector = JMXConnectorFactory.connect(url, null);
        connector.connect();
        MBeanServerConnection connection = connector.getMBeanServerConnection();
        ObjectName name = new ObjectName("org.apache.activemq:Type=Broker,BrokerName=localhost");
        BrokerViewMBean brokerMbean = (BrokerViewMBean) MBeanServerInvocationHandler.newProxyInstance(connection, name, BrokerViewMBean.class, true);
        Connection conn = createConnection();
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = sess.createConsumer(sess.createTopic("ActiveMQ.Advisory.Queue"));
        conn.start();
        Destination dest = sess.createQueue("test");

        brokerMbean.addQueue("test");

        ActiveMQMessage msg = (ActiveMQMessage)consumer.receive(1000);
        assertNotNull(msg);
        assertTrue(msg.getDataStructure() instanceof DestinationInfo);
        assertEquals(((DestinationInfo)msg.getDataStructure()).getDestination(), dest);
        assertEquals(((DestinationInfo)msg.getDataStructure()).getOperationType(), 0);

        brokerMbean.removeQueue("test");

        msg = (ActiveMQMessage)consumer.receive(1000);
        assertNotNull(msg);
        assertTrue(msg.getDataStructure() instanceof DestinationInfo);
        assertEquals(((DestinationInfo)msg.getDataStructure()).getDestination(), dest);
        assertEquals(((DestinationInfo)msg.getDataStructure()).getOperationType(), 1);


        brokerMbean.addQueue("test");
        msg = (ActiveMQMessage)consumer.receive(1000);
        assertNotNull(msg);
        assertTrue(msg.getDataStructure() instanceof DestinationInfo);
        assertEquals(((DestinationInfo)msg.getDataStructure()).getDestination(), dest);
        assertEquals(((DestinationInfo)msg.getDataStructure()).getOperationType(), 0);
        assertEquals(((DestinationInfo)msg.getDataStructure()).getOperationType(), 0);
    }


}
