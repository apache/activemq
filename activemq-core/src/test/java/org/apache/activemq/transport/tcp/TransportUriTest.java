/*
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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;

import javax.jms.Connection;

/**
 * 
 * @version $Revision$
 */
public class TransportUriTest extends EmbeddedBrokerTestSupport {

    private String postfix = "?tcpNoDelay=true&keepAlive=true";
    private Connection connection;

    public void testUriOptionsWork() throws Exception {
        String uri = bindAddress + postfix;
        System.out.println("Connecting via: " + uri);

        connection = new ActiveMQConnectionFactory(uri).createConnection();
        connection.start();
    }

    protected void setUp() throws Exception {
        bindAddress = "tcp://localhost:6161";
        super.setUp();
    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.tearDown();
    }

}
