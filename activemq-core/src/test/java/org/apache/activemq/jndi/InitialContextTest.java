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

import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version $Revision: 1.3 $
 */
public class InitialContextTest extends TestCase {

    private static final Logger LOG = LoggerFactory.getLogger(InitialContextTest.class);

    public void testInitialContext() throws Exception {
        InitialContext context = new InitialContext();
        assertTrue("Created context", context != null);

        ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory)context.lookup("ConnectionFactory");

        assertTrue("Should have created a ConnectionFactory", connectionFactory != null);

        LOG.info("Created with brokerURL: " + connectionFactory.getBrokerURL());

    }

    public void testUsingStandardJNDIKeys() throws Exception {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
        String expected = "tcp://localhost:65432";
        properties.put(Context.PROVIDER_URL, expected);

        InitialContext context = new InitialContext(properties);
        assertTrue("Created context", context != null);

        ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory)context.lookup("ConnectionFactory");

        assertTrue("Should have created a ConnectionFactory", connectionFactory != null);

        assertEquals("the brokerURL should match", expected, connectionFactory.getBrokerURL());
    }

    public void testConnectionFactoryPolicyConfig() throws Exception {

        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
        properties.put(Context.PROVIDER_URL, "tcp://localhost:65432");
        properties.put("prefetchPolicy.queuePrefetch", "777");
        properties.put("redeliveryPolicy.maximumRedeliveries", "15");
        properties.put("redeliveryPolicy.backOffMultiplier", "32");

        InitialContext context = new InitialContext(properties);
        assertTrue("Created context", context != null);

        ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory)context.lookup("ConnectionFactory");

        assertTrue("Should have created a ConnectionFactory", connectionFactory != null);

        assertEquals(777, connectionFactory.getPrefetchPolicy().getQueuePrefetch());
        assertEquals(15, connectionFactory.getRedeliveryPolicy().getMaximumRedeliveries());
        assertEquals(32d, connectionFactory.getRedeliveryPolicy().getBackOffMultiplier());
    }
}
