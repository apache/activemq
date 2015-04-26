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
package org.apache.activemq.transport.amqp.client;

import java.net.URI;

import org.apache.activemq.transport.amqp.AmqpTestSupport;

/**
 * Test support class for tests that will be using the AMQP Proton wrapper client.
 */
public class AmqpClientTestSupport extends AmqpTestSupport {

    public String getAmqpConnectionURIOptions() {
        return "";
    }

    public URI getBrokerAmqpConnectionURI() {
        try {
            String uri = "tcp://127.0.0.1:" + amqpPort;

            if (!getAmqpConnectionURIOptions().isEmpty()) {
                uri = uri + "?" + getAmqpConnectionURIOptions();
            }

            return new URI(uri);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    public AmqpConnection createAmqpConnection() throws Exception {
        return createAmqpConnection(getBrokerAmqpConnectionURI());
    }

    public AmqpConnection createAmqpConnection(String username, String password) throws Exception {
        return createAmqpConnection(getBrokerAmqpConnectionURI(), username, password);
    }

    public AmqpConnection createAmqpConnection(URI brokerURI) throws Exception {
        return createAmqpConnection(brokerURI, null, null);
    }

    public AmqpConnection createAmqpConnection(URI brokerURI, String username, String password) throws Exception {
        return createAmqpClient(brokerURI, username, password).connect();
    }

    public AmqpClient createAmqpClient() throws Exception {
        return createAmqpClient(getBrokerAmqpConnectionURI(), null, null);
    }

    public AmqpClient createAmqpClient(URI brokerURI) throws Exception {
        return createAmqpClient(brokerURI, null, null);
    }

    public AmqpClient createAmqpClient(String username, String password) throws Exception {
        return createAmqpClient(getBrokerAmqpConnectionURI(), username, password);
    }

    public AmqpClient createAmqpClient(URI brokerURI, String username, String password) throws Exception {
        return new AmqpClient(brokerURI, username, password);
    }
}
