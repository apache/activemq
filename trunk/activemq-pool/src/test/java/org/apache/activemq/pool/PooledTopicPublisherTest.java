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
package org.apache.activemq.pool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.test.TestSupport;
import org.apache.activemq.util.SocketProxy;

/**
 * 
 */
public class PooledTopicPublisherTest extends TestSupport {

    private TopicConnection connection;

    public void testPooledConnectionFactory() throws Exception {
        ActiveMQTopic topic = new ActiveMQTopic("test");
        PooledConnectionFactory pcf = new PooledConnectionFactory();
        pcf.setConnectionFactory(new ActiveMQConnectionFactory("vm://test"));

        connection = (TopicConnection) pcf.createConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicPublisher publisher = session.createPublisher(topic);
        publisher.publish(session.createMessage());
    }

    
    public void testSetGetExceptionListener() throws Exception {
        PooledConnectionFactory pcf = new PooledConnectionFactory();
        pcf.setConnectionFactory(new ActiveMQConnectionFactory("vm://test"));

        connection = (TopicConnection) pcf.createConnection();
        ExceptionListener listener = new ExceptionListener() {
            public void onException(JMSException exception) {
            }
        };
        connection.setExceptionListener(listener);
        assertEquals(listener, connection.getExceptionListener());
    }
    
    public void testPooledConnectionAfterInactivity() throws Exception {
        BrokerService broker = new BrokerService();
        TransportConnector networkConnector = broker.addConnector("tcp://localhost:0");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.start();
        
        SocketProxy proxy = new SocketProxy(networkConnector.getConnectUri());
        
        PooledConnectionFactory pcf = new PooledConnectionFactory();
        String uri = proxy.getUrl().toString() + "?trace=true&wireFormat.maxInactivityDuration=500&wireFormat.maxInactivityDurationInitalDelay=500";
        pcf.setConnectionFactory(new ActiveMQConnectionFactory(uri));
        
        PooledConnection conn =  (PooledConnection) pcf.createConnection();
        ActiveMQConnection amq = conn.getConnection();
        final CountDownLatch gotException = new CountDownLatch(1);
        //amq.set
        conn.setExceptionListener(new ExceptionListener() {
            public void onException(JMSException exception) {
                gotException.countDown();
            }});
        conn.setClientID(getName());
        
        // let it hang, simulate a server hang so inactivity timeout kicks in
        proxy.pause();
        //assertTrue("got an exception", gotException.await(5, TimeUnit.SECONDS));
        TimeUnit.SECONDS.sleep(2);
        conn.close();
    }
    
    @Override
    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }
}
