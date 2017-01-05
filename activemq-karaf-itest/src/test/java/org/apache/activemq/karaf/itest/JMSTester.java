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
package org.apache.activemq.karaf.itest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.Closeable;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.karaf.shell.api.console.SessionFactory;

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
public class JMSTester implements Closeable {
	
	private Connection connection;
	
	public JMSTester(Connection connection) {
		this.connection = connection;
	}

	public JMSTester() {
        try {
			ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
			connection = factory.createConnection(KarafShellHelper.USER, KarafShellHelper.PASSWORD);
			connection.start();
		} catch (JMSException e) {
			throw new RuntimeException(e);
		}
	}

    public String consumeMessage(String nameAndPayload) {
        try {
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			MessageConsumer consumer = session.createConsumer(session.createQueue(nameAndPayload));
			TextMessage message = (TextMessage) consumer.receive(10000);
			System.err.println("message: " + message);
			return message.getText();
		} catch (JMSException e) {
			throw new RuntimeException(e);
		}
    }

    public void produceMessage(String nameAndPayload) {
        try {
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			session.createProducer(session.createQueue(nameAndPayload)).send(session.createTextMessage(nameAndPayload));
		} catch (JMSException e) {
			throw new RuntimeException(e);
		}
    }
    
	public void produceAndConsume(SessionFactory sf) {
		final String nameAndPayload = String.valueOf(System.currentTimeMillis());
      	produceMessage(nameAndPayload);

        KarafShellHelper.executeCommand(sf, "activemq:bstat").trim();

        AbstractFeatureTest.withinReason(new Runnable() {
            public void run() {
                assertEquals("JMS_BODY_FIELD:JMSText = " + nameAndPayload, KarafShellHelper.executeCommand(sf, "activemq:browse --amqurl tcp://localhost:61616 --user karaf --password karaf -Vbody " + nameAndPayload).trim());
            }
        });

        assertEquals("got our message", nameAndPayload, consumeMessage(nameAndPayload));
	}
    
    public void tempSendReceive() throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryQueue temporaryQueue = session.createTemporaryQueue();
        session.createProducer(temporaryQueue).send(session.createTextMessage("TEST"));
        Message msg = session.createConsumer(temporaryQueue).receive(3000);
        assertNotNull("Didn't receive the message", msg);

    }

    public void close() {
    	try {
			connection.close();
		} catch (JMSException e) {
			throw new RuntimeException(e);
		}
	}
}