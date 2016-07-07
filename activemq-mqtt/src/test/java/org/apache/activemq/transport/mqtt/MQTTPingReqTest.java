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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.EOFException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;
import org.fusesource.hawtdispatch.DispatchQueue;
import org.fusesource.hawtdispatch.internal.SerialDispatchQueue;
import org.fusesource.hawtdispatch.transport.Transport;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.Callback;
import org.fusesource.mqtt.client.CallbackConnection;
import org.fusesource.mqtt.client.FutureConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Tracer;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.fusesource.mqtt.codec.MQTTProtocolCodec;
import org.fusesource.mqtt.codec.PINGREQ;
import org.fusesource.mqtt.codec.PINGRESP;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test to show that a PINGRESP will only be sent for a PINGREQ
 * packet after a CONNECT packet has been received.
 */
@RunWith(Parameterized.class)
public class MQTTPingReqTest extends MQTTTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTPingReqTest.class);

    @Rule
    public Timeout timeout = new Timeout(15, TimeUnit.SECONDS);

    private final String version;

    @Parameters(name = "mqtt-version:{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {"3.1"},
            {"3.1.1"}
        });
    }

    public MQTTPingReqTest(final String version) {
        this.version = version;
    }

    @Test(expected=EOFException.class)
    public void testPingReqWithoutConnectFail() throws Exception {
        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("clientId");
        mqtt.setVersion(version);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Transport> transport = new AtomicReference<>();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final Callback<Transport> con = new Callback<Transport>() {

            @Override
            public void onSuccess(Transport value) {
                transport.set(value);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable value) {
                error.set(value);
                latch.countDown();
            }
        };

        //Connect to the transport by using the createTransport method with a custom callback
        //This will ensure that we connect without sending a CONNECT packet for testing
        //and that we won't receive automatically
        CallbackConnection connection = new CallbackConnection(mqtt);
        Method createTransportMethod = connection.getClass().getDeclaredMethod("createTransport", Callback.class);
        createTransportMethod.setAccessible(true);
        createTransportMethod.invoke(connection, con);
        latch.await();

        //Make sure no error on connect
        if (error.get() != null) {
            LOG.error(error.get().getMessage(), error.get());
            fail(error.get().getMessage());
        }

        disableDispatchAssertion(transport.get());

        //Send a PINGREQ without a connect packet first
        final MQTTProtocolCodec codec = new MQTTProtocolCodec();
        codec.setTransport(transport.get());
        transport.get().offer(new PINGREQ().encode());

        //Protocol should throw an exception since we never sent a CONNECT
        Wait.waitFor(new Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                //Wait for exception to be thrown
                codec.read();
                return false;
            }
        }, 5000, 100);
    }

    @Test
    public void testPingReqConnectSuccess() throws Exception {
        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("clientId");
        mqtt.setVersion(version);

        final CountDownLatch pingRespReceived = new CountDownLatch(1);
        //Tracer to assert we received the response by waiting for it
        mqtt.setTracer(new Tracer() {

            @Override
            public void onReceive(MQTTFrame frame) {
                if (frame.messageType() == PINGRESP.TYPE) {
                    pingRespReceived.countDown();
                }
            }

        });
        CallbackConnection callbackConnection = new CallbackConnection(mqtt);
        BlockingConnection connection = new BlockingConnection(new FutureConnection(callbackConnection));
        connection.connect();
        Transport transport =  callbackConnection.transport();
        disableDispatchAssertion(transport);

        //SEND a PINGREQ and wait for the response
        final MQTTProtocolCodec codec = new MQTTProtocolCodec();
        codec.setTransport(transport);
        transport.offer(new PINGREQ().encode());

        //Wait for the response
        assertTrue(pingRespReceived.await(5, TimeUnit.SECONDS));
    }

    private void disableDispatchAssertion(final Transport transport) {
        //Since we are purposefully bypassing the normal way of sending a packet, turn off the
        //assertion
        DispatchQueue dispatchQueue = transport.getDispatchQueue();
        if (dispatchQueue instanceof SerialDispatchQueue) {
            SerialDispatchQueue spyQueue = Mockito.spy((SerialDispatchQueue)dispatchQueue);
            Mockito.doNothing().when(spyQueue).assertExecuting();
            transport.setDispatchQueue(spyQueue);
        }
    }
}
