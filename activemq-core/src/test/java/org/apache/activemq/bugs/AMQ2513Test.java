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
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.broker.jmx.ManagementContext;

/**
 * This unit test verifies an issue when
 * javax.management.InstanceNotFoundException is thrown after subsequent startups when
 * managementContext createConnector="false"
 *
 */
public class AMQ2513Test extends TestCase {

    private BrokerService broker;
    private String connectionUri;

    void createBroker(boolean deleteAllMessagesOnStartup) throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("localhost");
        broker.setUseJmx(true);
        broker.setDeleteAllMessagesOnStartup(deleteAllMessagesOnStartup);
        broker.addConnector("tcp://localhost:0");

        ManagementContext ctx = new ManagementContext();
        //if createConnector == true everything is fine
        ctx.setCreateConnector(false);
        broker.setManagementContext(ctx);

        broker.start();
        broker.waitUntilStarted();

        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
    }

    public void testJmx() throws Exception{
        createBroker(true);

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(session.createQueue("test"));
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        connection.start();

        producer.send(session.createTextMessage("test123"));

        DestinationViewMBean dv = createView();
        assertTrue(dv.getQueueSize() > 0);

        connection.close();

        broker.stop();
        broker.waitUntilStopped();

        createBroker(false);
        factory = new ActiveMQConnectionFactory(connectionUri);
        connection = factory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(session.createQueue("test"));
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        connection.start();
        producer.send(session.createTextMessage("test123"));
        connection.close();

        dv = createView();
        assertTrue(dv.getQueueSize() > 0);

        broker.stop();
        broker.waitUntilStopped();

    }

    DestinationViewMBean createView() throws Exception {
        String domain = "org.apache.activemq";
        ObjectName name = new ObjectName(domain + ":BrokerName=localhost,Type=Queue,Destination=test");
        return (DestinationViewMBean) broker.getManagementContext().newProxyInstance(name, DestinationViewMBean.class,
                true);
    }

}