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
package org.apache.activemq.transport.tcp;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.textui.TestRunner;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.TransportBrokerTestSupport;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 
 */
public class SslContextBrokerServiceTest extends TestCase {

    
    private ClassPathXmlApplicationContext context;
    private BrokerService broker;
    private TransportConnector connector;


    public void testConfiguration() throws URISyntaxException {

        assertNotNull(broker);
        assertNotNull(connector);
        
        assertEquals(new URI("ssl://localhost:61616"), connector.getUri());
        
        assertNotNull(broker.getSslContext());
        assertFalse(broker.getSslContext().getKeyManagers().isEmpty());
        assertFalse(broker.getSslContext().getTrustManagers().isEmpty());
        
    }

    protected void setUp() throws Exception {
        Thread.currentThread().setContextClassLoader(SslContextBrokerServiceTest.class.getClassLoader());
        context = new ClassPathXmlApplicationContext("org/apache/activemq/transport/tcp/activemq-ssl.xml");
        Map beansOfType = context.getBeansOfType(BrokerService.class);
        broker = (BrokerService)beansOfType.values().iterator().next();
        connector = broker.getTransportConnectors().get(0); 
    }
    
    @Override
    protected void tearDown() throws Exception {

        context.destroy();
    }

}
