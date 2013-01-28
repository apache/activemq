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
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;

import junit.framework.Assert;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AMQ2213Test
{
    BrokerService broker;
    ConnectionFactory factory;
    Connection connection;
    Session session;
    Queue queue;
    MessageConsumer consumer;

    public void createBroker(boolean deleteAll) throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(deleteAll);
        broker.setDataDirectory("target/AMQ3145Test");
        broker.setUseJmx(true);
        broker.getManagementContext().setCreateConnector(false);
        broker.addConnector("tcp://localhost:0");
        broker.start();
        broker.waitUntilStarted();
        factory = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getConnectUri().toString());
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
    }

    @Before
    public void createBroker() throws Exception {
        createBroker(true);
    }

    @After
    public void tearDown() throws Exception {
        if (consumer != null) {
            consumer.close();
        }
        session.close();
        connection.stop();
        connection.close();
        broker.stop();
    }

    @Test
    public void testEqualsGenericSession() throws JMSException
    {
        Assert.assertNotNull(this.connection);
        Session sess = this.connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Assert.assertTrue(sess.equals(sess));
    }

    @Test
    public void testEqualsTopicSession() throws JMSException
    {
        Assert.assertNotNull(this.connection);
        Assert.assertTrue(this.connection instanceof TopicConnection);
        TopicSession sess = ((TopicConnection)this.connection).createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        Assert.assertTrue(sess.equals(sess));
    }

    @Test
    public void testEqualsQueueSession() throws JMSException
    {
        Assert.assertNotNull(this.connection);
        Assert.assertTrue(this.connection instanceof QueueConnection);
        QueueSession sess = ((QueueConnection)this.connection).createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        Assert.assertTrue(sess.equals(sess));
    }
}
