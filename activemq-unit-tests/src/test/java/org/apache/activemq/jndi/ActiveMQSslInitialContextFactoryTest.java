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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.ActiveMQXASslConnectionFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ActiveMQSslInitialContextFactoryTest {

    protected Context context;
    protected boolean isXa;

    @Parameters(name = "isXa={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {true},
                {false}
        });
    }

    /**
     * @param isXa
     */
    public ActiveMQSslInitialContextFactoryTest(boolean isXa) {
        super();
        this.isXa = isXa;
    }

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
        environment.put("xa", Boolean.toString(isXa));

        context = factory.getInitialContext(environment);
        assertTrue("No context created", context != null);
    }

    @Test
    public void testCreateXaConnectionFactory() throws NamingException {
        ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory) context.lookup("ConnectionFactory");
        assertTrue(factory instanceof ActiveMQSslConnectionFactory);
        if (isXa) {
            assertTrue(factory instanceof ActiveMQXASslConnectionFactory);
        } else {
            assertFalse(factory instanceof ActiveMQXASslConnectionFactory);
        }
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
