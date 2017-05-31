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
package org.apache.activemq.transport.mqtt;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;

import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that the maxFrameSize configuration value is applied across the transports.
 */
@RunWith(Parameterized.class)
public class MQTTMaxFrameSizeTest extends MQTTTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTMaxFrameSizeTest.class);

    private final int maxFrameSize;

    @Parameters(name="{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { "mqtt", false, 1024 },
                { "mqtt+ssl", true, 1024 },
                { "mqtt+nio", false, 1024 },
                { "mqtt+nio+ssl", true, 1024 }
            });
    }

    public MQTTMaxFrameSizeTest(String connectorScheme, boolean useSSL, int maxFrameSize) {
        super(connectorScheme, useSSL);

        this.maxFrameSize = maxFrameSize;
    }

    @Override
    public String getProtocolConfig() {
        return "?wireFormat.maxFrameSize=" + maxFrameSize;
    }

    @Test(timeout = 30000)
    public void testFrameSizeToLargeClosesConnection() throws Exception {

        LOG.debug("Starting test on connector {} for frame size: {}", getProtocolScheme(), maxFrameSize);

        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId(getTestName());
        mqtt.setKeepAlive((short) 10);
        mqtt.setVersion("3.1.1");

        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        final int payloadSize = maxFrameSize + 100;

        byte[] payload = new byte[payloadSize];
        for (int i = 0; i < payloadSize; ++i) {
            payload[i] = 42;
        }

        try {
            connection.publish(getTopicName(), payload, QoS.AT_LEAST_ONCE, false);
            fail("should have thrown an exception");
        } catch (Exception ex) {
        } finally {
            connection.disconnect();
        }
    }

    @Test(timeout = 30000)
    public void testFrameSizeNotExceededWorks() throws Exception {

        LOG.debug("Starting test on connector {} for frame size: {}", getProtocolScheme(), maxFrameSize);

        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId(getTestName());
        mqtt.setKeepAlive((short) 10);
        mqtt.setVersion("3.1.1");

        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        final int payloadSize = maxFrameSize / 2;

        byte[] payload = new byte[payloadSize];
        for (int i = 0; i < payloadSize; ++i) {
            payload[i] = 42;
        }

        try {
            connection.publish(getTopicName(), payload, QoS.AT_LEAST_ONCE, false);
        } catch (Exception ex) {
            fail("should not have thrown an exception");
        } finally {
            connection.disconnect();
        }
    }
}
