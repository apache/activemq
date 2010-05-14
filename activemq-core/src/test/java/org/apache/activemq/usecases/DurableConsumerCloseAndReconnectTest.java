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
package org.apache.activemq.usecases;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.test.TestSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class DurableConsumerCloseAndReconnectTest extends TestSupport {
    protected static final long RECEIVE_TIMEOUT = 5000L;
    private static final Log LOG = LogFactory.getLog(DurableConsumerCloseAndReconnectTest.class);

    protected Connection connection;
    private Session session;
    private MessageConsumer consumer;
    private MessageProducer producer;
    private Destination destination;
    private int messageCount;

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://localhost?broker.deleteAllMessagesOnStartup=false");
    }

    public void testCreateDurableConsumerCloseThenReconnect() throws Exception {
        // force the server to stay up across both connection tests
        Connection dummyConnection = createConnection();
        dummyConnection.start();

        consumeMessagesDeliveredWhileConsumerClosed();

        dummyConnection.close();

        // now lets try again without one connection open
        consumeMessagesDeliveredWhileConsumerClosed();
        // now delete the db
        ActiveMQConnectionFactory fac = new ActiveMQConnectionFactory("vm://localhost?broker.deleteAllMessagesOnStartup=true");
        dummyConnection = fac.createConnection();
        dummyConnection.start();
        dummyConnection.close();
    }

    protected void consumeMessagesDeliveredWhileConsumerClosed() throws Exception {
        makeConsumer();
        closeConsumer();

        publish();

        // wait a few moments for the close to really occur
        Thread.sleep(1000);

        makeConsumer();

        Message message = consumer.receive(RECEIVE_TIMEOUT);
        assertTrue("Should have received a message!", message != null);

        closeConsumer();

        LOG.info("Now lets create the consumer again and because we didn't ack, we should get it again");
        makeConsumer();

        message = consumer.receive(RECEIVE_TIMEOUT);
        assertTrue("Should have received a message!", message != null);
        message.acknowledge();

        closeConsumer();

        LOG.info("Now lets create the consumer again and because we did ack, we should not get it again");
        makeConsumer();

        message = consumer.receive(2000);
        assertTrue("Should have no more messages left!", message == null);

        closeConsumer();

        LOG.info("Lets publish one more message now");
        publish();

        makeConsumer();
        message = consumer.receive(RECEIVE_TIMEOUT);
        assertTrue("Should have received a message!", message != null);
        message.acknowledge();

        closeConsumer();
    }

    protected void publish() throws Exception {
        connection = createConnection();
        connection.start();

        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        destination = createDestination();

        producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        TextMessage msg = session.createTextMessage("This is a test: " + messageCount++);
        producer.send(msg);

        producer.close();
        producer = null;
        closeSession();
    }

    protected Destination createDestination() throws JMSException {
        if (isTopic()) {
            return session.createTopic(getSubject());
        } else {
            return session.createQueue(getSubject());
        }
    }

    protected boolean isTopic() {
        return true;
    }

    protected void closeConsumer() throws JMSException {
        consumer.close();
        consumer = null;
        closeSession();
    }

    protected void closeSession() throws JMSException {
        session.close();
        session = null;
        connection.close();
        connection = null;
    }

    protected void makeConsumer() throws Exception {
        String durableName = getName();
        String clientID = getSubject();
        LOG.info("Creating a durable subscribe for clientID: " + clientID + " and durable name: " + durableName);
        createSession(clientID);
        consumer = createConsumer(durableName);
    }

    private MessageConsumer createConsumer(String durableName) throws JMSException {
        if (destination instanceof Topic) {
            return session.createDurableSubscriber((Topic)destination, durableName);
        } else {
            return session.createConsumer(destination);
        }
    }

    protected void createSession(String clientID) throws Exception {
        connection = createConnection();
        connection.setClientID(clientID);
        connection.start();

        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        destination = createDestination();
    }
}
