/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.spring;

import java.util.Arrays;

import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class ActiveMQConnectionFactoryFactoryBeanTest extends TestCase {
    private static final transient Logger LOG = LoggerFactory.getLogger(ActiveMQConnectionFactoryFactoryBeanTest.class);

    private ActiveMQConnectionFactoryFactoryBean factory;


    public void testSingleTcpURL() throws Exception {
        factory.setTcpHostAndPort("tcp://localhost:61616");
        assertCreatedURL("failover:(tcp://localhost:61616)");
    }

    public void testSingleTcpURLWithInactivityTimeout() throws Exception {
        factory.setTcpHostAndPort("tcp://localhost:61616");
        factory.setMaxInactivityDuration(60000L);
        assertCreatedURL("failover:(tcp://localhost:61616?wireFormat.maxInactivityDuration=60000)");
    }

    public void testSingleTcpURLWithInactivityTimeoutAndTcpNoDelay() throws Exception {
        factory.setTcpHostAndPort("tcp://localhost:61616");
        factory.setMaxInactivityDuration(50000L);
        factory.setTcpProperties("tcpNoDelayEnabled=true");
        assertCreatedURL("failover:(tcp://localhost:61616?wireFormat.maxInactivityDuration=50000&tcpNoDelayEnabled=true)");
    }

    public void testSingleTcpURLWithInactivityTimeoutAndMaxReconnectDelay() throws Exception {
        factory.setTcpHostAndPort("tcp://localhost:61616");
        factory.setMaxInactivityDuration(60000L);
        factory.setMaxReconnectDelay(50000L);
        assertCreatedURL("failover:(tcp://localhost:61616?wireFormat.maxInactivityDuration=60000)?maxReconnectDelay=50000");
    }

    public void testSingleTcpURLWithInactivityTimeoutAndMaxReconnectDelayAndFailoverProperty() throws Exception {
        factory.setTcpHostAndPort("tcp://localhost:61616");
        factory.setMaxInactivityDuration(40000L);
        factory.setMaxReconnectDelay(30000L);
        factory.setFailoverProperties("useExponentialBackOff=false");

        assertCreatedURL("failover:(tcp://localhost:61616?wireFormat.maxInactivityDuration=40000)?maxReconnectDelay=30000&useExponentialBackOff=false");
    }

    public void testMultipleTcpURLsWithInactivityTimeoutAndMaxReconnectDelayAndFailoverProperty() throws Exception {
        factory.setTcpHostAndPorts(Arrays.asList(new String[] {"tcp://localhost:61618", "tcp://foo:61619"}));
        factory.setMaxInactivityDuration(40000L);
        factory.setMaxReconnectDelay(30000L);
        factory.setFailoverProperties("useExponentialBackOff=false");

        assertCreatedURL("failover:(tcp://localhost:61618?wireFormat.maxInactivityDuration=40000,tcp://foo:61619?wireFormat.maxInactivityDuration=40000)?maxReconnectDelay=30000&useExponentialBackOff=false");
    }

    protected void assertCreatedURL(String expectedURL) throws Exception {
        String url = factory.getBrokerURL();
        LOG.debug("Generated URL: " + url);

        assertEquals("URL", expectedURL, url);
        Object value = factory.getObject();
        assertTrue("Value should be an ActiveMQConnectionFactory", value instanceof ActiveMQConnectionFactory);
        ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory) value;
        String brokerURL = connectionFactory.getBrokerURL();
        assertEquals("brokerURL", expectedURL, brokerURL);
    }

    @Override
    protected void setUp() throws Exception {
        factory = new ActiveMQConnectionFactoryFactoryBean();
    }
}
