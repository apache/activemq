/**
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
package org.apache.activemq.transport.amqp;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.junit.Test;

public class JMSParallelConnectTest extends AmqpTestSupport {

    @Override
    protected boolean isUseTcpConnector() {
        return true;
    }

    @Override
    protected boolean isUseSslConnector() {
        return true;
    }

    @Override
    protected boolean isUseNioConnector() {
        return true;
    }

    @Override
    protected boolean isUseNioPlusSslConnector() {
        return true;
    }

    @Test(timeout = 60000)
    public void testParallelConnectPlain() throws Exception {
        final int numThreads = 40;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {

                    try {
                        Connection connection = JMSClientContext.INSTANCE.createConnection(amqpURI, "admin", "password");
                        connection.start();
                        connection.close();
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        executorService.shutdown();
        assertTrue("executor done on time", executorService.awaitTermination(30, TimeUnit.SECONDS));
    }

    @Test(timeout = 60000)
    public void testParallelConnectNio() throws Exception {
        final int numThreads = 40;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {

                    try {
                        Connection connection = JMSClientContext.INSTANCE.createConnection(amqpNioURI, "admin", "password");
                        connection.start();
                        connection.close();
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        executorService.shutdown();
        assertTrue("executor done on time", executorService.awaitTermination(30, TimeUnit.SECONDS));
    }

    @Test(timeout = 60000)
    public void testParallelConnectSsl() throws Exception {
        final int numThreads = 40;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {

                    try {
                        Connection connection = JMSClientContext.INSTANCE.createConnection(amqpSslURI, "admin", "password");
                        connection.start();
                        connection.close();
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        executorService.shutdown();
        assertTrue("executor done on time", executorService.awaitTermination(30, TimeUnit.SECONDS));
    }

    @Test(timeout = 60000)
    public void testParallelConnectNioPlusSsl() throws Exception {
        final int numThreads = 40;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {

                    try {
                        Connection connection = JMSClientContext.INSTANCE.createConnection(amqpNioPlusSslURI, "admin", "password");
                        connection.start();
                        connection.close();
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        executorService.shutdown();
        assertTrue("executor done on time", executorService.awaitTermination(30, TimeUnit.SECONDS));
    }
}
