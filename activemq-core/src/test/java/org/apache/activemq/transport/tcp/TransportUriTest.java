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

import javax.jms.Connection;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;

/**
 * @version $Revision$
 */
public class TransportUriTest extends EmbeddedBrokerTestSupport {

    protected String prefix = "";
    protected String postfix = "?tcpNoDelay=true&keepAlive=true";
    protected Connection connection;

    public void testUriOptionsWork() throws Exception {
        String uri = prefix + bindAddress + postfix;
        // System.out.println("Connecting via: " + uri);

        connection = new ActiveMQConnectionFactory(uri).createConnection();
        connection.start();
    }

    public void testBadVersionNumberDoesNotWork() throws Exception {
        String uri = prefix + bindAddress + postfix + "&minmumWireFormatVersion=65535";
        // System.out.println("Connecting via: " + uri);

        try {
            connection = new ActiveMQConnectionFactory(uri).createConnection();
            connection.start();
            fail("Should have thrown an exception!");
        } catch (Exception expected) {
        }
    }

    public void testBadPropertyNameFails() throws Exception {
        String uri = prefix + bindAddress + postfix + "&cheese=abc";
        // System.out.println("Connecting via: " + uri);

        try {
            connection = new ActiveMQConnectionFactory(uri).createConnection();
            connection.start();
            fail("Should have thrown an exception!");
        } catch (Exception expected) {
        }
    }

    protected void setUp() throws Exception {
        bindAddress = "tcp://localhost:61616";
        super.setUp();
    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
        super.tearDown();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseJmx(false);
        answer.setPersistent(isPersistent());
        answer.addConnector(bindAddress);
        return answer;
    }
}
