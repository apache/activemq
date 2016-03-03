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
package org.apache.activemq.transport.mqtt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.activemq.broker.BrokerService;
import org.fusesource.mqtt.codec.CONNACK;
import org.fusesource.mqtt.codec.CONNECT;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Tests for various usage scenarios of the protocol converter
 */
public class MQTTProtocolConverterTest {

    private MQTTTransport transport;
    private BrokerService broker;

    @Before
    public void setUp() throws Exception {
        transport = Mockito.mock(MQTTTransport.class);
        broker = Mockito.mock(BrokerService.class);
    }

    @Test
    public void testConnectWithInvalidProtocolVersionToLow() throws IOException {
        doTestConnectWithInvalidProtocolVersion(2);
    }

    @Test
    public void testConnectWithInvalidProtocolVersionToHigh() throws IOException {
        doTestConnectWithInvalidProtocolVersion(5);
    }

    private void doTestConnectWithInvalidProtocolVersion(int version) throws IOException {
        MQTTProtocolConverter converter = new MQTTProtocolConverter(transport, broker);

        CONNECT connect = Mockito.mock(CONNECT.class);

        Mockito.when(connect.version()).thenReturn(version);

        converter.onMQTTConnect(connect);
        ArgumentCaptor<IOException> capturedException = ArgumentCaptor.forClass(IOException.class);
        Mockito.verify(transport).onException(capturedException.capture());

        assertTrue(capturedException.getValue().getMessage().contains("version"));

        ArgumentCaptor<MQTTFrame> capturedFrame = ArgumentCaptor.forClass(MQTTFrame.class);
        Mockito.verify(transport).sendToMQTT(capturedFrame.capture());

        MQTTFrame response = capturedFrame.getValue();
        assertEquals(CONNACK.TYPE, response.messageType());

        CONNACK connAck = new CONNACK().decode(response);
        assertEquals(CONNACK.Code.CONNECTION_REFUSED_UNACCEPTED_PROTOCOL_VERSION, connAck.code());
    }
}
