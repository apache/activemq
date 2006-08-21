/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activemq.usecases;

import javax.jms.*;
import java.util.HashMap;
import java.util.Collections;
import java.util.Map;
import java.net.URI;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.TransportConnector;

/**
* This Unit Test is created to test the memory leakage when a new producer is created and closed for each message that was sent.
* The error occured after sending and receiving messages 65536 times.
* This test validate if the messages sent by producer is being received by the consumer.
* A new producer is created and closed for each message that was sent.
* The said procedure is done fore more than 65536 times.
*/


public final class NoDestinationErrorTest extends TestCase {
    public static final String ACTIVEMQ_SERVER = "ActiveMQ Server";
    public static final boolean TRANSACTED_FALSE = false;
    public static final String TOOL_DEFAULT = "TOOL.DEFAULT";
    public static final String LAST_MESSAGE = "LAST";

    public String userName;
    public String password;

    private MessageProducer producer;
    private MessageConsumer consumer;

    public static Map ProducerMap = Collections.synchronizedMap(new HashMap());
    protected BrokerService broker;
    private boolean isTopic = false;

    protected ConnectionFactory factory;
    protected ActiveMQConnection connection;


    public NoDestinationErrorTest() {
        super();
    }

    public void doTest() throws Exception {
        ConnectionFactory factory = createConnectionFactory();
        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection(userName, password);
        connection.start();

        Session session = createSession(connection, TRANSACTED_FALSE);
        Destination destination = createDestination(session, "subject", isTopic);
        consumer = session.createConsumer(destination);

        for (int i=0; i<70000; i++) {
            TextMessage sentMessage;
            sentMessage = session.createTextMessage("message " + i);
            producer = session.createProducer(destination);
            producer.send(sentMessage);
            producer.close();

            TextMessage rcvMessage = null;
            rcvMessage = (TextMessage)consumer.receive(i);
            String message = rcvMessage.getText();
            assertNotNull("Message received should not be null", message);
            System.out.println(message);
        }

        connection.close();
    }

    /**
     *  Creates the connection to the broker.
     *
     *  @return Connection - broker connection.
     */
    protected ConnectionFactory createConnectionFactory() throws JMSException {
        return new ActiveMQConnectionFactory("vm://localhost?broker.persistent=true");
    }

    /**
     * Creates the connection session.
     *
     * @param connection   - broker connection.
     * @param isTransacted - true if the session will be session transacted.
     *                     otherwise the the session will be using auto acknowledge.
     * @return Session - connection session.
     */
    private static Session createSession(Connection connection,
                                         boolean isTransacted) throws JMSException {
        if (isTransacted) {
            return connection.createSession(true, Session.SESSION_TRANSACTED);
        } else {
            return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        }
    }

    /**
     * Creates the session destination.
     *
     * @param session - connection session.
     * @param subject - destination name.
     * @param isTopic - true if the destination is a topic,
     *                otherwise the destination is a queue.
     * @return Destination - session destination.
     */
    private static Destination createDestination(Session session,
                                                 String subject,
                                                 boolean isTopic) throws JMSException {
        if (isTopic) {
            return session.createTopic(subject);
        } else {
            return session.createQueue(subject);
        }
    }

    public void testSendReceive() throws Exception {
        doTest();
    }

}
