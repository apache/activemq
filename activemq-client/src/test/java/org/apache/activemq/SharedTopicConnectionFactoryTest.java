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

import java.net.URI;

import jakarta.jms.Connection;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.transport.Transport;
import org.junit.Test;

public class SharedTopicConnectionFactoryTest {

    @Test
    public void testIsInstanceOfActiveMQConnectionFactory() {
        SharedTopicConnectionFactory factory = new SharedTopicConnectionFactory();
        assertTrue(factory instanceof ActiveMQConnectionFactory);
    }

    @Test
    public void testStringConstructor() {
        SharedTopicConnectionFactory factory =
                new SharedTopicConnectionFactory("tcp://localhost:61616");
        assertEquals("tcp://localhost:61616", factory.getBrokerURL());
    }

    @Test
    public void testUriConstructor() throws Exception {
        SharedTopicConnectionFactory factory =
                new SharedTopicConnectionFactory(new URI("tcp://localhost:61616"));
        assertEquals("tcp://localhost:61616", factory.getBrokerURL());
    }

    @Test
    public void testCredentialConstructors() throws Exception {
        SharedTopicConnectionFactory f1 =
                new SharedTopicConnectionFactory("admin", "secret",
                        new URI("tcp://localhost:61616"));
        assertEquals("admin", f1.getUserName());

        SharedTopicConnectionFactory f2 =
                new SharedTopicConnectionFactory("admin", "secret",
                        "tcp://localhost:61616");
        assertEquals("admin", f2.getUserName());
    }

    @Test
    public void testCreatesSharedTopicConnection() throws Exception {
        SharedTopicConnectionFactory factory = createStubFactory();
        Connection conn = factory.createConnection();
        try {
            assertTrue("Factory should create SharedTopicConnection",
                    conn instanceof SharedTopicConnection);
        } finally {
            conn.close();
        }
    }

    @Test
    public void testCreatesSharedTopicConnectionWithCredentials() throws Exception {
        SharedTopicConnectionFactory factory = createStubFactory();
        Connection conn = factory.createConnection("user", "pass");
        try {
            assertTrue(conn instanceof SharedTopicConnection);
        } finally {
            conn.close();
        }
    }

    static SharedTopicConnectionFactory createStubFactory() {
        return new SharedTopicConnectionFactory("tcp://localhost:61616") {
            @Override
            protected Transport createTransport() {
                return new StubTransport();
            }
        };
    }
}
