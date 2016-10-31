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
package org.apache.activemq.jndi;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ActiveMQSslInitialContextFactoryTest {

    protected Context context;

    @Before
    public void setUp() throws Exception {
        InitialContextFactory factory = new ActiveMQSslInitialContextFactory();
        Hashtable<String, String> environment = new Hashtable<String, String>();
        environment.put("java.naming.provider.url", "vm://0");
        environment.put("connection.ConnectionFactory.userName", "user");
        environment.put("connection.ConnectionFactory.userPassword", "test");
        environment.put("connection.ConnectionFactory.keyStore", "keystore.jks");
        environment.put("connection.ConnectionFactory.keyStorePassword", "test");
        environment.put("connection.ConnectionFactory.keyStoreType", "JKS");
        environment.put("connection.ConnectionFactory.trustStore", "truststore.jks");
        environment.put("connection.ConnectionFactory.trustStorePassword", "test");
        environment.put("connection.ConnectionFactory.trustStoreType", "JKS");
        
        context = factory.getInitialContext(environment);
        assertTrue("No context created", context != null);
    }

    @Test
    public void testCreateConnectionFactory() throws NamingException {
        assertTrue(context.lookup("ConnectionFactory") instanceof ActiveMQSslConnectionFactory);
    }

    @Test
    public void testAssertConnectionFactoryProperties() throws NamingException {
        Object c = context.lookup("ConnectionFactory");
        if (c instanceof ActiveMQSslConnectionFactory) {
            ActiveMQSslConnectionFactory factory = (ActiveMQSslConnectionFactory)c;
            assertEquals(factory.getKeyStore(), "keystore.jks");
            assertEquals(factory.getKeyStorePassword(), "test");
            assertEquals(factory.getKeyStoreType(), "JKS");
            assertEquals(factory.getTrustStore(), "truststore.jks");
            assertEquals(factory.getTrustStorePassword(), "test");
            assertEquals(factory.getTrustStoreType(), "JKS");
        } else {
            fail("Did not find an ActiveMQSslConnectionFactory");
        }
    }

}
