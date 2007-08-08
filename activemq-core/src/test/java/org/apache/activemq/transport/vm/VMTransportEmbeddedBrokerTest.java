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
package org.apache.activemq.transport.vm;

import java.net.URI;
import java.net.URISyntaxException;

import javax.jms.DeliveryMode;

import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerTestSupport;
import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.util.IOExceptionSupport;

/**
 * Used to see if the VM transport starts an embedded broker on demand.
 * 
 * @version $Revision$
 */
public class VMTransportEmbeddedBrokerTest extends BrokerTestSupport {

    public static void main(String[] args) {
        junit.textui.TestRunner.run(VMTransportEmbeddedBrokerTest.class);
    }

    public void testConsumerPrefetchAtOne() throws Exception {
        
        // Make sure the broker is created due to the connection being started.
        assertNull(BrokerRegistry.getInstance().lookup("localhost"));        
        StubConnection connection = createConnection();
        assertNotNull(BrokerRegistry.getInstance().lookup("localhost"));

        // Start a producer and consumer
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);
        
        ActiveMQQueue destination = new ActiveMQQueue("TEST");

        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);  
        consumerInfo.setPrefetchSize(1);
        connection.send(consumerInfo);
        
        // Send 2 messages to the broker.
        connection.send(createMessage(producerInfo, destination, DeliveryMode.NON_PERSISTENT));
        connection.send(createMessage(producerInfo, destination, DeliveryMode.NON_PERSISTENT));
        
        // Make sure only 1 message was delivered.
        Message m = receiveMessage(connection);
        assertNotNull(m);
        assertNoMessagesLeft(connection);
        
        // Make sure the broker is shutdown when the connection is stopped.
        assertNotNull(BrokerRegistry.getInstance().lookup("localhost"));        
        connection.stop();
        assertNull(BrokerRegistry.getInstance().lookup("localhost"));        
    }

    protected void setUp() throws Exception {
        // Don't call super since it manually starts up a broker.
    }
    protected void tearDown() throws Exception {
        // Don't call super since it manually tears down a broker.
    }
    protected StubConnection createConnection() throws Exception {
        try {
            Transport transport = TransportFactory.connect(new URI("vm://localhost?broker.persistent=false"));
            StubConnection connection = new StubConnection(transport);
            return connection;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

}
