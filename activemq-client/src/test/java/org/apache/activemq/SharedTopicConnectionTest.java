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

import static org.junit.Assert.*;

import jakarta.jms.Connection;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SharedTopicConnectionTest {

    private Connection connection;

    @Before
    public void setUp() throws Exception {
        SharedTopicConnectionFactory factory =
                SharedTopicConnectionFactoryTest.createStubFactory();
        connection = factory.createConnection();
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void testIsInstanceOfActiveMQConnection() {
        assertTrue(connection instanceof ActiveMQConnection);
        assertTrue(connection instanceof SharedTopicConnection);
    }

    @Test
    public void testCreateSessionReturnsSharedTopicSession() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertTrue("createSession should return SharedTopicSession",
                session instanceof SharedTopicSession);
    }

    @Test
    public void testCreateTransactedSession() throws Exception {
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        assertTrue(session instanceof SharedTopicSession);
    }

    @Test
    public void testCreateSessionNoArgs() throws Exception {
        Session session = connection.createSession();
        assertTrue("No-arg createSession should also return SharedTopicSession",
                session instanceof SharedTopicSession);
    }

    @Test
    public void testCreateSessionSingleArg() throws Exception {
        Session session = connection.createSession(Session.CLIENT_ACKNOWLEDGE);
        assertTrue(session instanceof SharedTopicSession);
    }

    @Test(expected = jakarta.jms.JMSException.class)
    public void testInvalidAcknowledgeMode() throws Exception {
        connection.createSession(false, Session.SESSION_TRANSACTED);
    }
}
