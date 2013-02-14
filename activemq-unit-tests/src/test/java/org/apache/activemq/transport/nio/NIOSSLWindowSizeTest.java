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
package org.apache.activemq.transport.nio;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

@SuppressWarnings("javadoc")
public class NIOSSLWindowSizeTest extends TestCase {
	
    BrokerService broker;
    Connection connection;
    Session session;
    
    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/server.keystore";
    public static final String TRUST_KEYSTORE = "src/test/resources/client.keystore";

    public static final int PRODUCER_COUNT = 1;
    public static final int CONSUMER_COUNT = 1;
    public static final int MESSAGE_COUNT = 1;
    public static final int MESSAGE_SIZE = 65536;

    byte[] messageData;
    
    @Override
    protected void setUp() throws Exception {
        System.setProperty("javax.net.ssl.trustStore", TRUST_KEYSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStore", SERVER_KEYSTORE);
        System.setProperty("javax.net.ssl.keyStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);

        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        TransportConnector connector = broker.addConnector("nio+ssl://localhost:0?transport.needClientAuth=true");
        broker.start();
        broker.waitUntilStarted();
        
        messageData = new byte[MESSAGE_SIZE];
        for (int i = 0; i < MESSAGE_SIZE;  i++)
        {
        	messageData[i] = (byte) (i & 0xff);
        }
        
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("nio+ssl://localhost:" + connector.getConnectUri().getPort());
        connection = factory.createConnection();
        session = connection.createSession(false,  Session.AUTO_ACKNOWLEDGE);        
        connection.start();
    }

    @Override
    protected void tearDown() throws Exception {
    	if (session != null) {
    		session.close();
    	}
        if (connection != null) {
            connection.close();
        }

        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    public void testLargePayload() throws Exception {
        Queue dest = session.createQueue("TEST");
    	MessageProducer prod = null;
        try {
        	prod = session.createProducer(dest);
        	BytesMessage msg = session.createBytesMessage();
        	msg.writeBytes(messageData);
        	prod.send(msg);
        } finally {
        	prod.close();
        }        
    	MessageConsumer cons = null;
    	try 
    	{
    		cons = session.createConsumer(dest);
    		assertNotNull(cons.receive(30000L));
        } finally {
        	cons.close();
        }        
    }
}
