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
package org.apache.activemq.transport.vm;

import java.net.URI;

import javax.jms.Connection;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerRegistry;

public class VMTransportBrokerNameTest extends TestCase {

    private static final String MY_BROKER = "myBroker";
    final String vmUrl = "vm:(broker:(tcp://localhost:61616)/" + MY_BROKER + "?persistent=false)";

    public void testBrokerName() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(new URI(vmUrl));
        ActiveMQConnection c1 = (ActiveMQConnection) cf.createConnection();
        assertTrue("Transport has name in it: " + c1.getTransport(), c1.getTransport().toString().contains(MY_BROKER));
        
        // verify Broker is there with name
        ActiveMQConnectionFactory cfbyName = new ActiveMQConnectionFactory(new URI("vm://" + MY_BROKER + "?create=false"));
        Connection c2 = cfbyName.createConnection();
        
        assertNotNull(BrokerRegistry.getInstance().lookup(MY_BROKER));
        assertEquals(BrokerRegistry.getInstance().findFirst().getBrokerName(), MY_BROKER);
        assertEquals(BrokerRegistry.getInstance().getBrokers().size(), 1);
        
        c1.close();
        c2.close();
    }
}
