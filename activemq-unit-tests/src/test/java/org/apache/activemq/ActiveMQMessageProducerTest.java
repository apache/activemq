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
import jakarta.jms.Topic;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.command.DataStructureTestSupport.assertEquals;
import static org.junit.Assert.fail;

public class ActiveMQMessageProducerTest {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQMessageProducerTest.class);

    @Test
    public void testStrictComplianceDeliveryDelay() throws Exception {
        // Use vm transport for fast testing
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");

        // Test WITH Strict Compliance
        factory.setStrictCompliance(true);
        try (Connection connection = factory.createConnection()) {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic("Test.Topic");
            MessageProducer producer = session.createProducer(topic);

            try {
                producer.setDeliveryDelay(-500);
                fail("Should have thrown MessageFormatException for negative delay");
            } catch (jakarta.jms.MessageFormatException e) {
                LOG.debug("Caught expected exception: {}", e.getMessage());
            }
        }

        // Test WITHOUT Strict Compliance (Should allow negative for legacy/non-strict)
        factory.setStrictCompliance(false);
        try (Connection connection = factory.createConnection()) {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic("Test.Topic");
            MessageProducer producer = session.createProducer(topic);

            // This should NOT throw an exception now
            producer.setDeliveryDelay(-500);
            assertEquals(-500, producer.getDeliveryDelay());
        }
    }

}
