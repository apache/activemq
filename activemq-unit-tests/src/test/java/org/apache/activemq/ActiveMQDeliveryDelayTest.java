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

import static org.junit.Assert.assertEquals;
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
        // Turn OFF strict compliance
        factory.setStrictCompliance(false);

        try (Connection conn = factory.createConnection();
             Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            MessageProducer producer = sess.createProducer(sess.createQueue("TEST.LEGACY"));

            // We set -1000L
            producer.setDeliveryDelay(-1000L);

            // clamp this to 0 internally.
            // The test should verify that the broker "corrected" the value.
            assertEquals("Negative delay should be clamped to 0 in legacy mode",
                    0L, producer.getDeliveryDelay());
        }
    }

    @Test
    public void testDeliveryDelayEffectiveOnMessage() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        try (Connection conn = factory.createConnection();
             Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            MessageProducer producer = sess.createProducer(sess.createQueue("TEST.EFFECTIVE"));

            ActiveMQMessage msg = (ActiveMQMessage) sess.createTextMessage("Hello");

            long delay = 5000L;
            producer.setDeliveryDelay(delay);

            // Record the absolute start
            final long before = System.currentTimeMillis();

            producer.send(msg);

            final long after = System.currentTimeMillis();

            long deliveryTime = msg.getJMSDeliveryTime();

            // Use a 100ms buffer.
            // This accounts for OS clock jitter while still proving the 5000ms delay was applied.
            assertTrue("Delivery time (" + deliveryTime + ") is too early! Expected >= " + (before + delay),
                    deliveryTime >= (before + delay - 100));

            assertTrue("Delivery time (" + deliveryTime + ") is too late! Expected <= " + (after + delay),
                    deliveryTime <= (after + delay + 100));
        }
    }
}
