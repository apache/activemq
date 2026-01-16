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
import static org.mockito.Mockito.times;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.transport.mqtt.strategy.MQTTSubscriptionStrategy;
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

    @Test
    public void testConcurrentOnTransportError() throws Exception {
        MQTTProtocolConverter converter = new MQTTProtocolConverter(transport, broker);
        converter.setSubsciptionStrategy(Mockito.mock(MQTTSubscriptionStrategy.class));

        CONNECT connect = Mockito.mock(CONNECT.class);

        Mockito.when(connect.version()).thenReturn(3);
        Mockito.when(connect.cleanSession()).thenReturn(true);

        converter.onMQTTConnect(connect);

        ArgumentCaptor<ConnectionInfo> connectionInfoArgumentCaptor = ArgumentCaptor.forClass(ConnectionInfo.class);
        Mockito.verify(transport).sendToActiveMQ(connectionInfoArgumentCaptor.capture());

        ConnectionInfo connectInfo = connectionInfoArgumentCaptor.getValue();
        Response ok = new Response();
        ok.setCorrelationId(connectInfo.getCommandId());
        converter.onActiveMQCommand(ok);

        ArgumentCaptor<Command> producerInfoArgumentCaptor = ArgumentCaptor.forClass(Command.class);
        Mockito.verify(transport, times(3)).sendToActiveMQ(producerInfoArgumentCaptor.capture());

        ProducerInfo producerInfo = (ProducerInfo) producerInfoArgumentCaptor.getValue();
        ok = new Response();
        ok.setCorrelationId(producerInfo.getCommandId());
        converter.onActiveMQCommand(ok);

        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i=0; i<10; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    converter.onTransportError();
                }
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);

        ArgumentCaptor<RemoveInfo> removeInfo = ArgumentCaptor.forClass(RemoveInfo.class);
        Mockito.verify(transport, times(4)).sendToActiveMQ(removeInfo.capture());

    }
}
