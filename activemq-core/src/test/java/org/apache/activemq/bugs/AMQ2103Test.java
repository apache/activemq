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
package org.apache.activemq.bugs;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import junit.framework.Test;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerTestSupport;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.usecases.MyObject;

public class AMQ2103Test extends BrokerTestSupport {
    static PolicyEntry reduceMemoryFootprint = new PolicyEntry();
    static {
        reduceMemoryFootprint.setReduceMemoryFootprint(true);
    }

    public PolicyEntry defaultPolicy = reduceMemoryFootprint;

    @Override
    protected PolicyEntry getDefaultPolicy() {
        return defaultPolicy;
    }

    public void initCombosForTestVerifyMarshalledStateIsCleared() throws Exception {
        addCombinationValues("defaultPolicy", new Object[]{defaultPolicy, null});    
    }

    public static Test suite() {
        return suite(AMQ2103Test.class);
    }

    /**
     * use mem persistence so no marshaling,
     * reduceMemoryFootprint on/off that will reduce memory by whacking the marshaled state
     * With vm transport and deferred serialisation and no persistence (mem persistence),
     * we see the message as sent by the client so we can validate the contents against
     * the policy
     * @throws Exception
     */
    public void testVerifyMarshalledStateIsCleared() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        factory.setOptimizedMessageDispatch(true);
        factory.setObjectMessageSerializationDefered(true);
        factory.setCopyMessageOnSend(false);

        Connection connection = factory.createConnection();
        Session session = (ActiveMQSession)connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQDestination destination = new ActiveMQQueue("testQ");
		MessageConsumer consumer = session.createConsumer(destination);
		connection.start();

        MessageProducer producer = session.createProducer(destination);
        final MyObject obj = new MyObject("A message");
        ActiveMQObjectMessage m1 = (ActiveMQObjectMessage)session.createObjectMessage();
        m1.setObject(obj);
        producer.send(m1);

        ActiveMQTextMessage m2 = new ActiveMQTextMessage();
        m2.setText("Test Message Payload.");
        producer.send(m2);

        ActiveMQMapMessage m3 = new ActiveMQMapMessage();
        m3.setString("text", "my message");
        producer.send(m3);

        Message m = consumer.receive(maxWait);
        assertNotNull(m);
        assertEquals(m1.getMessageId().toString(), m.getJMSMessageID());
        assertTrue(m instanceof ActiveMQObjectMessage);

        if (getDefaultPolicy() != null) {
            assertNull("object data cleared by reduceMemoryFootprint (and never marshalled as using mem persistence)",
                ((ActiveMQObjectMessage)m).getObject());
        }

        // verify no serialisation via vm transport
        assertEquals("writeObject called", 0, obj.getWriteObjectCalled());
        assertEquals("readObject called", 0, obj.getReadObjectCalled());
        assertEquals("readObjectNoData called", 0, obj.getReadObjectNoDataCalled());

        m = consumer.receive(maxWait);
        assertNotNull(m);
        assertEquals(m2.getMessageId().toString(), m.getJMSMessageID());
        assertTrue(m instanceof ActiveMQTextMessage);

        if (getDefaultPolicy() != null) {
            assertNull("text cleared by reduceMemoryFootprint (and never marshalled as using mem persistence)",
                ((ActiveMQTextMessage)m).getText());
        }

        m = consumer.receive(maxWait);
        assertNotNull(m);
        assertEquals(m3.getMessageId().toString(), m.getJMSMessageID());
        assertTrue(m instanceof ActiveMQMapMessage);

        if (getDefaultPolicy() != null) {
            assertNull("text cleared by reduceMemoryFootprint (and never marshalled as using mem persistence)",
                ((ActiveMQMapMessage)m).getStringProperty("text"));
        }

        connection.close();
    }
}
