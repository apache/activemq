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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;

/**
 * Test cases used to test the JMS message consumer.
 * 
 * 
 */
public class JmsTestSupport extends CombinationTestSupport {

    static final private AtomicLong TEST_COUNTER = new AtomicLong();
    public String userName;
    public String password;
    public String messageTextPrefix = "";

    protected ConnectionFactory factory;
    protected ActiveMQConnection connection;
    protected BrokerService broker;

    protected List<Connection> connections = Collections.synchronizedList(new ArrayList<Connection>());

    // /////////////////////////////////////////////////////////////////
    //
    // Test support methods.
    //
    // /////////////////////////////////////////////////////////////////
    protected ActiveMQDestination createDestination(Session session, byte type) throws JMSException {
        String testMethod = getName();
        if( testMethod.indexOf(" ")>0 ) {
            testMethod = testMethod.substring(0, testMethod.indexOf(" "));
        }
        String name = "TEST." + getClass().getName() + "." +testMethod+"."+TEST_COUNTER.getAndIncrement();
        switch (type) {
        case ActiveMQDestination.QUEUE_TYPE:
            return (ActiveMQDestination)session.createQueue(name);
        case ActiveMQDestination.TOPIC_TYPE:
            return (ActiveMQDestination)session.createTopic(name);
        case ActiveMQDestination.TEMP_QUEUE_TYPE:
            return (ActiveMQDestination)session.createTemporaryQueue();
        case ActiveMQDestination.TEMP_TOPIC_TYPE:
            return (ActiveMQDestination)session.createTemporaryTopic();
        default:
            throw new IllegalArgumentException("type: " + type);
        }
    }

    protected void sendMessages(Destination destination, int count) throws Exception {
        ConnectionFactory factory = createConnectionFactory();
        Connection connection = factory.createConnection();
        connection.start();
        sendMessages(connection, destination, count);
        connection.close();
    }

    protected void sendMessages(Connection connection, Destination destination, int count) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        sendMessages(session, destination, count);
        session.close();
    }

    protected void sendMessages(Session session, Destination destination, int count) throws JMSException {
        MessageProducer producer = session.createProducer(destination);
        for (int i = 0; i < count; i++) {
            producer.send(session.createTextMessage(messageTextPrefix  + i));
        }
        producer.close();
    }

    protected ConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://localhost");
    }

    protected BrokerService createBroker() throws Exception {
        return BrokerFactory.createBroker(new URI("broker://()/localhost?persistent=false"));
    }

    protected void setUp() throws Exception {
        super.setUp();

        if (System.getProperty("basedir") == null) {
            File file = new File(".");
            System.setProperty("basedir", file.getAbsolutePath());
        }

        broker = createBroker();
        broker.start();
        factory = createConnectionFactory();
        connection = (ActiveMQConnection)factory.createConnection(userName, password);
        connections.add(connection);
    }

    protected void tearDown() throws Exception {
        for (Iterator iter = connections.iterator(); iter.hasNext();) {
            Connection conn = (Connection)iter.next();
            try {
                conn.close();
            } catch (Throwable e) {
            }
            iter.remove();
        }
        broker.stop();
        super.tearDown();
    }

    protected void safeClose(Connection c) {
        try {
            c.close();
        } catch (Throwable e) {
        }
    }

    protected void safeClose(Session s) {
        try {
            s.close();
        } catch (Throwable e) {
        }
    }

    protected void safeClose(MessageConsumer c) {
        try {
            c.close();
        } catch (Throwable e) {
        }
    }

    protected void safeClose(MessageProducer p) {
        try {
            p.close();
        } catch (Throwable e) {
        }
    }

    protected void profilerPause(String prompt) throws IOException {
        if (System.getProperty("profiler") != null) {
            pause(prompt);
        }
    }

    protected void pause(String prompt) throws IOException {
        System.out.println();
        System.out.println(prompt + "> Press enter to continue: ");
        while (System.in.read() != '\n') {
        }
    }

}
