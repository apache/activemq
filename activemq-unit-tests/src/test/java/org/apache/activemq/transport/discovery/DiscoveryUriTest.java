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

package org.apache.activemq.transport.discovery;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;

import javax.jms.*;
import java.net.URI;

public class DiscoveryUriTest extends EmbeddedBrokerTestSupport {

    @Override
    protected BrokerService createBroker() throws Exception {
        bindAddress = "tcp://localhost:0";
        BrokerService answer = new BrokerService();
        answer.setPersistent(isPersistent());
        TransportConnector connector = new TransportConnector();
        connector.setUri(new URI(bindAddress));
        connector.setDiscoveryUri(new URI("multicast://default?group=test"));
        answer.addConnector(connector);
        return answer;
    }

    public void testConnect() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("discovery:(multicast://default?group=test)?reconnectDelay=1000&maxReconnectAttempts=30&useExponentialBackOff=false");
        Connection conn = factory.createConnection();
        conn.start();

        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = sess.createProducer(sess.createQueue("test"));
        producer.send(sess.createTextMessage("test"));
        MessageConsumer consumer = sess.createConsumer(sess.createQueue("test"));
        Message msg = consumer.receive(1000);
        assertNotNull(msg);
    }

    public void testFailedConnect() throws Exception {
        try {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("discovery:(multicast://default?group=test1)?reconnectDelay=1000&startupMaxReconnectAttempts=3&useExponentialBackOff=false");
            Connection conn = factory.createConnection();
            conn.start();
        } catch (Exception e) {
            return;
        }
        fail("Expected connection failure");
    }
}
