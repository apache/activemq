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
package org.apache.activemq.transport.amqp.interop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpConnectionListener;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test that the maxFrameSize setting prevents large frames from being processed.
 */
@RunWith(Parameterized.class)
public class AmqpMaxFrameSizeTest extends AmqpClientTestSupport {

    private final int TEST_IDLE_TIMEOUT = 500;

    private final String testName;
    private final int maxFrameSize;
    private final int maxAmqpFrameSize;

    @Parameters(name="{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { "amqp-> MFS > MAFS", "amqp", false, 1024, 2048 },
                { "amqp-> MFS < MAFS", "amqp", false, 2048, 1024 },
                { "amqp+nio-> MFS > MAFS", "amqp+nio", false, 1024, 2048 },
                { "amqp+nio-> MFS < MAFS", "amqp+nio", false, 2048, 1024 },
                { "amqp+ws-> MFS > MAFS", "amqp+ws", false, 1024, 2048 },
                { "amqp+ws-> MFS < MAFS", "amqp+ws", false, 2048, 1024 },
            });
    }

    public AmqpMaxFrameSizeTest(String testName, String connectorScheme, boolean useSSL, int maxFrameSize, int maxAmqpFrameSize) {
        super(connectorScheme, useSSL);

        this.testName = testName;
        this.maxFrameSize = maxFrameSize;
        this.maxAmqpFrameSize = maxAmqpFrameSize;
    }

    @Override
    protected String getAdditionalConfig() {
        return "&wireFormat.maxAmqpFrameSize=" + maxAmqpFrameSize +
               "&wireFormat.maxFrameSize=" + maxFrameSize;
    }

    @Test(timeout = 600000)
    public void testMaxFrameSizeApplied() throws Exception {

        LOG.info("Test starting {} for transport {} with MFS:{} and MAFS:{}",
            new Object[]{ testName, getConnectorScheme(), maxFrameSize, maxAmqpFrameSize });

        final CountDownLatch failed = new CountDownLatch(1);

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.createConnection());
        connection.setListener(new AmqpConnectionListener() {

            @Override
            public void onException(Throwable ex) {
                failed.countDown();
            }
        });

        connection.setIdleTimeout(TEST_IDLE_TIMEOUT);
        connection.connect();

        AmqpSession session = connection.createSession();
        AmqpSender sender = session.createSender("queue://" + getTestName(), true);

        byte[] payload = new byte[maxFrameSize * 2];
        for (int i = 0; i < payload.length; ++i) {
            payload[i] = 42;
        }

        AmqpMessage message = new AmqpMessage();
        message.setBytes(payload);

        sender.send(message);

        assertTrue("Connection should have failed", failed.await(30, TimeUnit.SECONDS));

        assertNotNull(getProxyToQueue(getTestName()));
        assertEquals(0, getProxyToQueue(getTestName()).getQueueSize());

        connection.close();
    }
}
