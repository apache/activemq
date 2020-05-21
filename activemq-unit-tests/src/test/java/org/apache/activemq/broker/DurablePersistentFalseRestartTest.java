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
package org.apache.activemq.broker;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import junit.framework.Test;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.transport.failover.FailoverTransport;

public class DurablePersistentFalseRestartTest extends BrokerRestartTestSupport {

    @Override
    protected void configureBroker(BrokerService broker) throws Exception {
        super.configureBroker(broker);
        broker.setPersistent(false);
        broker.setPersistenceAdapter(new KahaDBPersistenceAdapter());
        broker.addConnector("tcp://0.0.0.0:0");
    }

    public void testValidateNoPersistenceForDurableAfterRestart() throws Exception {

        ConnectionFactory connectionFactory =
                new ActiveMQConnectionFactory("failover:(" + broker.getTransportConnectors().get(0).getPublishableConnectString() + ")");
        ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.setClientID("clientId");
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic destination = session.createTopic(queueName);
        MessageConsumer consumer = session.createDurableSubscriber(destination, "subscriberName");

        populateDestination(10, destination, connection);

        restartBroker();

        // make failover aware of the restarted auto assigned port
        ((FailoverTransport) connection.getTransport().narrow(FailoverTransport.class)).add(true, broker.getTransportConnectors().get(0).getPublishableConnectString());

        TextMessage msg = (TextMessage) consumer.receive(8000);
        assertNull("did not get a message when persistent=false, message: " + msg, msg);

        connection.close();
    }

    private void populateDestination(final int nbMessages,
                                     final Destination destination, javax.jms.Connection connection)
            throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        for (int i = 1; i <= nbMessages; i++) {
            producer.send(session.createTextMessage("<hello id='" + i + "'/>"));
        }
        producer.close();
        session.close();
    }


    public static Test suite() {
        return suite(DurablePersistentFalseRestartTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
