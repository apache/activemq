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
package org.apache.activemq.transport.discovery;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;

public class DiscoveryTransportNoBrokerTest extends CombinationTestSupport {

    public void testMaxReconnectAttempts() throws JMSException {
        long start = System.currentTimeMillis();
        try {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("discovery:(multicast://doesNOTexist)?maxReconnectAttempts=1");
            System.out.println("Connecting.");
            Connection connection = factory.createConnection();
            connection.setClientID("test");
            fail("Did not fail to connect as expected.");
        } catch ( JMSException expected ) {       
            long duration = System.currentTimeMillis() - start;
            // Should have failed fairly quickly since we are only giving it 1 reconnect attempt.
            assertTrue(duration < 1000*5 );
        }
    }

}
