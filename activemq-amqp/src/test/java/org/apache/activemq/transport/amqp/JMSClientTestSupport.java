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

import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.junit.After;

public class JMSClientTestSupport extends AmqpTestSupport {

    protected Connection connection;

    private Thread connectionCloseThread;

    @Override
    @After
    public void tearDown() throws Exception {
        Future<Boolean> future = testService.submit(new CloseConnectionTask());
        try {
            LOG.debug("tearDown started.");
            future.get(60, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            if (connectionCloseThread != null) {
                connectionCloseThread.interrupt();;
            }

            testService.shutdownNow();
            testService = Executors.newSingleThreadExecutor();
            throw new Exception("CloseConnection timed out");
        } finally {
            connectionCloseThread = null;
            connection = null;
            super.tearDown();
        }
    }

    public class CloseConnectionTask implements Callable<Boolean> {
        @Override
        public Boolean call() throws Exception {
            if (connection != null) {
                connectionCloseThread = Thread.currentThread();
                LOG.debug("in CloseConnectionTask.call(), calling connection.close()");
                connection.close();
            }

            return Boolean.TRUE;
        }
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
     * @return the URI to connect to on the Broker for AMQP.
     */
    protected URI getBrokerURI() {
        return amqpURI;
    }

    protected Connection createConnection() throws JMSException {
        return createConnection(name.toString(), false);
    }

    protected Connection createConnection(boolean syncPublish) throws JMSException {
        return createConnection(name.toString(), syncPublish);
    }

    protected Connection createConnection(String clientId) throws JMSException {
        return createConnection(clientId, false);
    }

    protected Connection createConnection(String clientId, boolean syncPublish) throws JMSException {
        Connection connection = JMSClientContext.INSTANCE.createConnection(getBrokerURI(), "admin", "password", clientId, syncPublish);
        connection.start();
        return connection;
    }
}
