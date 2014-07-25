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
package org.apache.activemq.transport.amqp;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;

import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;
import org.junit.After;

public class JMSClientTestSupport extends AmqpTestSupport {

    protected Connection connection;

    @Override
    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
            }
        }

        super.tearDown();
    }

    /**
     * @return the proper destination name to use for each test method invocation.
     */
    protected String getDestinationName() {
        return name.getMethodName();
    }

    /**
     * Can be overridden in subclasses to test against a different transport suchs as NIO.
     *
     * @return the port to connect to on the Broker.
     */
    protected int getBrokerPort() {
        return port;
    }

    protected Connection createConnection() throws JMSException {
        return createConnection(name.toString(), false, false);
    }

    protected Connection createConnection(boolean syncPublish) throws JMSException {
        return createConnection(name.toString(), syncPublish, false);
    }

    protected Connection createConnection(String clientId) throws JMSException {
        return createConnection(clientId, false, false);
    }

    protected Connection createConnection(String clientId, boolean syncPublish, boolean useSsl) throws JMSException {

        int brokerPort = getBrokerPort();
        LOG.debug("Creating connection on port {}", brokerPort);
        final ConnectionFactoryImpl factory = new ConnectionFactoryImpl("localhost", brokerPort, "admin", "password", null, useSsl);

        factory.setSyncPublish(syncPublish);
        factory.setTopicPrefix("topic://");
        factory.setQueuePrefix("queue://");

        final Connection connection = factory.createConnection();
        if (clientId != null && !clientId.isEmpty()) {
            connection.setClientID(clientId);
        }
        connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
                exception.printStackTrace();
            }
        });
        connection.start();
        return connection;
    }
}
