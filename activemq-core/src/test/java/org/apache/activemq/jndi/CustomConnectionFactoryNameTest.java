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

import javax.naming.NamingException;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * Test case for AMQ-141
 *
 * @version $Revision: 1.2 $
 */
public class CustomConnectionFactoryNameTest extends ActiveMQInitialContextFactoryTest {
    
    public void testConnectionFactoriesArePresent() throws NamingException {
        super.testConnectionFactoriesArePresent();
        assertConnectionFactoryPresent("jms/Connection");
        assertConnectionFactoryPresent("jms/DURABLE_SUB_CONNECTION_FACTORY");
    }
    
    public void testConnectionFactoriesAreConfigured() throws NamingException {
        super.testConnectionFactoriesArePresent();
        ActiveMQConnectionFactory factory1 = (ActiveMQConnectionFactory) context.lookup("jms/Connection");
        assertNull(factory1.getClientID());
        ActiveMQConnectionFactory factory2 = (ActiveMQConnectionFactory) context.lookup("jms/DURABLE_SUB_CONNECTION_FACTORY");
        assertEquals("testclient", factory2.getClientID());
    }

    protected String getConnectionFactoryLookupName() {
        return "myConnectionFactory";
    }

    protected void configureEnvironment() {
        super.configureEnvironment();
        environment.put("connectionFactoryNames", " myConnectionFactory, jms/Connection, jms/DURABLE_SUB_CONNECTION_FACTORY");
        environment.put("connection.jms/DURABLE_SUB_CONNECTION_FACTORY.clientID", "testclient");
    }
}
