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
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.command.DataStructureTestSupport.assertEquals;
import static org.junit.Assert.fail;

public class ActiveMQMessageProducerTest {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQMessageProducerTest.class);

    @Test
    public void testStrictComplianceDeliveryDelay() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");

        // Strict Mode ON (Should block negative values)
        factory.setStrictCompliance(true);
        try (Connection conn = factory.createConnection()) {
            MessageProducer producer = conn.createSession(false, 1).createProducer(null);
            try {
                producer.setDeliveryDelay(-100);
                fail("Should have thrown MessageFormatException");
            } catch (jakarta.jms.MessageFormatException e) {
                LOG.debug("Caught expected strict compliance exception");
            }
        }

        // Strict Mode OFF (Should allow negative values (Legacy behavior))
        factory.setStrictCompliance(false);
        try (Connection conn = factory.createConnection()) {
            MessageProducer producer = conn.createSession(false, 1).createProducer(null);
            producer.setDeliveryDelay(-100);
            assertEquals(-100, producer.getDeliveryDelay());
        }
    }
}
