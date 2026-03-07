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
import jakarta.jms.Message;
import jakarta.jms.MessageFormatException;
import jakarta.jms.Session;
import org.junit.Test;
import static org.junit.Assert.fail;

public class ActiveMQMessagePropertyTest {

    @Test
    public void testSetObjectPropertyCompliance() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");

        // Test 1: Strict Mode (TCK Scenario)
        factory.setNestedMapAndListEnabled(false);
        Connection strictConn = factory.createConnection();
        Session strictSession = strictConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Message strictMsg = strictSession.createMessage();

        try {
            strictMsg.setObjectProperty("charProp", 'A');
            fail("Strict mode (TCK) should reject Character type");
        } catch (MessageFormatException e) {
            // Correct for Jakarta 3.1
        }
        strictConn.close();

        // Test 2: Legacy Mode (Backward Compatibility Scenario)
        factory.setNestedMapAndListEnabled(true);
        Connection legacyConn = factory.createConnection();
        Session legacySession = legacyConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Message legacyMsg = legacySession.createMessage();

        try {
            legacyMsg.setObjectProperty("charProp", 'A');
            // legacy support is preserved!
        } catch (MessageFormatException e) {
            fail("Legacy mode should still allow Character to avoid breaking existing users");
        }
        legacyConn.close();
    }

    @Test
    public void testLegacyValidationAllowsNonSpecTypes() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");

        // Default ActiveMQ legacy behavior
        factory.setNestedMapAndListEnabled(true);

        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Message message = session.createMessage();

        // Validates that Character/Map types are still accepted when legacy support is enabled.
        try {
            message.setObjectProperty("charProperty", 'A');
            message.setObjectProperty("mapProperty", new java.util.HashMap<>());
        } catch (MessageFormatException e) {
            fail("Legacy mode should allow Character and Map types, but threw: " + e.getMessage());
        }

        connection.close();
    }


}
