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

import static org.junit.Assert.fail;

import javax.jms.JMSException;
import javax.jms.Session;

public abstract class AbstractAuthorizationTest extends RuntimeConfigTestSupport {

    protected void assertDeniedTemp(String userPass) {
        try {
            assertAllowedTemp(userPass);
            fail("Expected not allowed exception");
        } catch (Exception expected) {
            LOG.debug("got:" + expected, expected);
        }
    }

    protected void assertAllowedTemp(String userPass) throws Exception {
        ActiveMQConnection connection = new ActiveMQConnectionFactory("vm://localhost").createActiveMQConnection(userPass, userPass);
        connection.start();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createTemporaryQueue());
        } finally {
            connection.close();
        }

    }

    protected void assertDenied(String userPass, String destination) {
        try {
            assertAllowed(userPass, destination);
            fail("Expected not allowed exception");
        } catch (JMSException expected) {
            LOG.debug("got:" + expected, expected);
        }
    }

    protected void assertAllowed(String userPass, String dest) throws JMSException {
        ActiveMQConnection connection = new ActiveMQConnectionFactory("vm://localhost").createActiveMQConnection(userPass, userPass);
        connection.start();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue(dest));
        } finally {
            connection.close();
        }
    }

    protected void assertAllowedWrite(String userPass, String dest) throws JMSException {
        ActiveMQConnection connection = new ActiveMQConnectionFactory("vm://localhost").createActiveMQConnection(userPass, userPass);
        connection.start();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createProducer(session.createQueue(dest)).send(session.createTextMessage());
        } finally {
            connection.close();
        }
    }

    protected void assertDeniedWrite(String userPass, String destination) {
        try {
            assertAllowedWrite(userPass, destination);
            fail("Expected not allowed exception");
        } catch (JMSException expected) {
            LOG.debug("got:" + expected, expected);
        }
    }


}
