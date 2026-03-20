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

import jakarta.jms.Connection;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import org.apache.activemq.command.ActiveMQMessage;
import org.junit.Test;

import static org.apache.activemq.command.DataStructureTestSupport.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ActiveMQDeliveryDelayTest {

    private final String connectionUri = "vm://localhost?broker.persistent=false";

    @Test
    public void testStrictComplianceRejectsNegativeDelay() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        // Turn ON strict compliance (Jakarta 3.1 requirement)
        factory.setStrictCompliance(true);

        try (Connection conn = factory.createConnection();
             Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            MessageProducer producer = sess.createProducer(sess.createQueue("TEST.STRICT"));

            try {
                producer.setDeliveryDelay(-1000L);
                fail("Should have thrown a JMSException for negative delay in strict mode");
            } catch (jakarta.jms.JMSException e) {
                // Success: Exception was thrown as required by the spec
            }
        }
    }

    @Test
    public void testLegacyBehaviorAllowsNegativeDelay() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        // Turn OFF strict compliance (Legacy ActiveMQ behavior)
        factory.setStrictCompliance(false);

        try (Connection conn = factory.createConnection();
             Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            MessageProducer producer = sess.createProducer(sess.createQueue("TEST.LEGACY"));

            // Should NOT throw an exception
            producer.setDeliveryDelay(-1000L);
            assertEquals(-1000L, producer.getDeliveryDelay());
        }
    }

    @Test
    public void testDeliveryDelayEffectiveOnMessage() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        try (Connection conn = factory.createConnection();
             Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            MessageProducer producer = sess.createProducer(sess.createQueue("TEST.EFFECTIVE"));
            long delay = 5000L;
            producer.setDeliveryDelay(delay);

            ActiveMQMessage msg = (ActiveMQMessage) sess.createTextMessage("Hello");
            producer.send(msg);

            // Verify Broker-side scheduling property
            assertEquals("Broker delay property missing",
                    delay, msg.getLongProperty("AMQ_SCHEDULED_DELAY"));

            // Verify Consumer-side visibility property (matching #1157 logic)
            assertTrue("JMSDeliveryTime property missing or incorrect",
                    msg.getLongProperty(ActiveMQMessage.JMS_DELIVERY_TIME_PROPERTY) >= System.currentTimeMillis() + delay - 100);
        }
    }
}
