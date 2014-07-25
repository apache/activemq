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
package org.apache.activemq.transport.amqp.bugs;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.JMSException;
import org.apache.activemq.transport.amqp.AmqpTestSupport;
import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;
import org.junit.Test;


import static org.junit.Assert.assertTrue;

public class AMQ5256Test extends AmqpTestSupport {

    @Override
    protected boolean isUseTcpConnector() {
        return true;
    }

    @Override
    protected boolean isUseNioPlusSslConnector() {
        return false;
    }

    @Test(timeout = 40 * 1000)
    public void testParallelConnect() throws Exception {
        final int numThreads = 80;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {

                    try {
                        final ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl("localhost", port, "admin", "password", null, isUseSslConnector());
                        Connection connection = connectionFactory.createConnection();
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
