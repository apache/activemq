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
package org.apache.activemq.transport.failover;

import java.net.URI;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;

public class FailoverTimeoutTest extends TestCase {

    private static final String QUEUE_NAME = "test.failovertimeout";
    BrokerService bs;
    URI tcpUri;

    public void setUp() throws Exception {
        bs = new BrokerService();
        bs.setUseJmx(false);
        bs.addConnector("tcp://localhost:0");
        bs.start();
        tcpUri = bs.getTransportConnectors().get(0).getConnectUri();
    }

    public void tearDown() throws Exception {
        if (bs != null) {
            bs.stop();
        }
    }

    public void testTimeout() throws Exception {

        long timeout = 1000;
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + tcpUri + ")?timeout=" + timeout + "&useExponentialBackOff=false");
        Connection connection = cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(session
                .createQueue(QUEUE_NAME));
        TextMessage message = session.createTextMessage("Test message");
        producer.send(message);

        bs.stop();

        try {
            producer.send(message);
        } catch (JMSException jmse) {
            assertEquals("Failover timeout of " + timeout + " ms reached.", jmse.getMessage());
        }

        bs = new BrokerService();
        bs.setUseJmx(false);
        bs.addConnector(tcpUri);
        bs.start();
        bs.waitUntilStarted();

        producer.send(message);

        bs.stop();
    }

    public void testUpdateUris() throws Exception {

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + tcpUri + ")?useExponentialBackOff=false");
        ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();
        connection.start();
        FailoverTransport failoverTransport = connection.getTransport().narrow(FailoverTransport.class);

        URI[] bunchOfUnknownAndOneKnown = new URI[]{
                new URI("tcp://unknownHost:" + tcpUri.getPort()),
                new URI("tcp://unknownHost2:" + tcpUri.getPort()),
                new URI("tcp://localhost:2222")};
        failoverTransport.add(false, bunchOfUnknownAndOneKnown);
    }
}
