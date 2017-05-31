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
package org.apache.activemq.jms.pool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.SocketProxy;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Test;

public class PooledTopicPublisherTest extends JmsPoolTestSupport {

    private TopicConnection connection;
    private PooledConnectionFactory pcf;

    @Override
    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception ex) {}
            connection = null;
        }

        if (pcf != null) {
            try {
                pcf.stop();
            } catch (Exception ex) {}
        }

        super.tearDown();
    }

    @Test(timeout = 60000)
    public void testPooledConnectionFactory() throws Exception {
        ActiveMQTopic topic = new ActiveMQTopic("test");
        pcf = new PooledConnectionFactory();
        pcf.setConnectionFactory(new ActiveMQConnectionFactory(
            "vm://test?broker.persistent=false&broker.useJmx=false"));

        connection = (TopicConnection) pcf.createConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicPublisher publisher = session.createPublisher(topic);
        publisher.publish(session.createMessage());
    }

    @Test(timeout = 60000)
    public void testSetGetExceptionListener() throws Exception {
        pcf = new PooledConnectionFactory();
        pcf.setConnectionFactory(new ActiveMQConnectionFactory(
            "vm://test?broker.persistent=false&broker.useJmx=false"));

        connection = (TopicConnection) pcf.createConnection();
        ExceptionListener listener = new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
            }
        };
        connection.setExceptionListener(listener);
        assertEquals(listener, connection.getExceptionListener());
    }

    @Test(timeout = 60000)
    public void testPooledConnectionAfterInactivity() throws Exception {
        brokerService = new BrokerService();
        TransportConnector networkConnector = brokerService.addConnector("tcp://localhost:0");
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);
        brokerService.getManagementContext().setCreateConnector(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setSchedulerSupport(false);
        brokerService.start();

        SocketProxy proxy = new SocketProxy(networkConnector.getConnectUri());

        pcf = new PooledConnectionFactory();
        String uri = proxy.getUrl().toString() + "?trace=true&wireFormat.maxInactivityDuration=500&wireFormat.maxInactivityDurationInitalDelay=500";
        pcf.setConnectionFactory(new ActiveMQConnectionFactory(uri));

        PooledConnection conn =  (PooledConnection) pcf.createConnection();
        Connection amq = conn.getConnection();
        assertNotNull(amq);
        final CountDownLatch gotException = new CountDownLatch(1);
        conn.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
                gotException.countDown();
            }});
        conn.setClientID(getTestName());

        // let it hang, simulate a server hang so inactivity timeout kicks in
        proxy.pause();

        assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getAdminView().getCurrentConnectionsCount() == 0;
            }
        }, TimeUnit.SECONDS.toMillis(15), TimeUnit.MILLISECONDS.toMillis(100)));

        conn.close();
    }
}
