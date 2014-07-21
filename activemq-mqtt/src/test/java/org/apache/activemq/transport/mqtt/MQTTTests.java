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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.net.ProtocolException;

import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Tracer;
import org.fusesource.mqtt.codec.CONNACK;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTTTests extends MQTTTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTTests.class);

    @Test(timeout = 60 * 1000)
    public void testBadUserNameOrPasswordGetsConnAckWithErrorCode() throws Exception {
        MQTT mqttPub = createMQTTConnection("pub", true);
        mqttPub.setUserName("admin");
        mqttPub.setPassword("admin");

        mqttPub.setTracer(new Tracer() {
            @Override
            public void onReceive(MQTTFrame frame) {
                LOG.info("Client received: {}", frame);
                if (frame.messageType() == CONNACK.TYPE) {
                    CONNACK connAck = new CONNACK();
                    try {
                        connAck.decode(frame);
                        LOG.info("{}", connAck);
                        assertEquals(CONNACK.Code.CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD, connAck.code());
                    } catch (ProtocolException e) {
                        fail("Error decoding publish " + e.getMessage());
                    }
                }
            }

            @Override
            public void onSend(MQTTFrame frame) {
                LOG.info("Client sent: {}", frame);
            }
        });

        BlockingConnection connectionPub = mqttPub.blockingConnection();
        try {
            connectionPub.connect();
            fail("Should not be able to connect.");
        } catch (Exception e) {}
    }
}
