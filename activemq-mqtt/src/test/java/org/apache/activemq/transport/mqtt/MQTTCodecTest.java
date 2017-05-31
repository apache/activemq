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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.DataByteArrayInputStream;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.fusesource.mqtt.codec.CONNECT;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.fusesource.mqtt.codec.PUBLISH;
import org.fusesource.mqtt.codec.SUBSCRIBE;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the functionality of the MQTTCodec class.
 */
public class MQTTCodecTest {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTCodecTest.class);

    private final MQTTWireFormat wireFormat = new MQTTWireFormat();

    private List<MQTTFrame> frames;
    private MQTTCodec codec;

    private final int MESSAGE_SIZE = 5 * 1024 * 1024;
    private final int ITERATIONS = 500;

    @Before
    public void setUp() throws Exception {
        frames = new ArrayList<MQTTFrame>();
        codec = new MQTTCodec(new MQTTCodec.MQTTFrameSink() {

            @Override
            public void onFrame(MQTTFrame mqttFrame) {
                frames.add(mqttFrame);
            }
        });
    }

    @Test
    public void testEmptyConnectBytes() throws Exception {

        CONNECT connect = new CONNECT();
        connect.cleanSession(true);
        connect.clientId(new UTF8Buffer(""));

        DataByteArrayOutputStream output = new DataByteArrayOutputStream();
        wireFormat.marshal(connect.encode(), output);
        Buffer marshalled = output.toBuffer();

        DataByteArrayInputStream input = new DataByteArrayInputStream(marshalled);
        codec.parse(input, marshalled.length());

        assertTrue(!frames.isEmpty());
        assertEquals(1, frames.size());

        connect = new CONNECT().decode(frames.get(0));
        LOG.info("Unmarshalled: {}", connect);
        assertTrue(connect.cleanSession());
    }

    @Test
    public void testConnectThenSubscribe() throws Exception {

        CONNECT connect = new CONNECT();
        connect.cleanSession(true);
        connect.clientId(new UTF8Buffer(""));

        DataByteArrayOutputStream output = new DataByteArrayOutputStream();
        wireFormat.marshal(connect.encode(), output);
        Buffer marshalled = output.toBuffer();

        DataByteArrayInputStream input = new DataByteArrayInputStream(marshalled);
        codec.parse(input, marshalled.length());

        assertTrue(!frames.isEmpty());
        assertEquals(1, frames.size());

        connect = new CONNECT().decode(frames.get(0));
        LOG.info("Unmarshalled: {}", connect);
        assertTrue(connect.cleanSession());

        frames.clear();

        SUBSCRIBE subscribe = new SUBSCRIBE();
        subscribe.topics(new Topic[] {new Topic("TEST", QoS.EXACTLY_ONCE) });

        output = new DataByteArrayOutputStream();
        wireFormat.marshal(subscribe.encode(), output);
        marshalled = output.toBuffer();

        input = new DataByteArrayInputStream(marshalled);
        codec.parse(input, marshalled.length());

        assertTrue(!frames.isEmpty());
        assertEquals(1, frames.size());

        subscribe = new SUBSCRIBE().decode(frames.get(0));
    }

    @Test
    public void testConnectWithCredentialsBackToBack() throws Exception {

        CONNECT connect = new CONNECT();
        connect.cleanSession(false);
        connect.clientId(new UTF8Buffer("test"));
        connect.userName(new UTF8Buffer("user"));
        connect.password(new UTF8Buffer("pass"));

        DataByteArrayOutputStream output = new DataByteArrayOutputStream();
        wireFormat.marshal(connect.encode(), output);
        wireFormat.marshal(connect.encode(), output);
        Buffer marshalled = output.toBuffer();

        DataByteArrayInputStream input = new DataByteArrayInputStream(marshalled);
        codec.parse(input, marshalled.length());

        assertTrue(!frames.isEmpty());
        assertEquals(2, frames.size());

        for (MQTTFrame frame : frames) {
            connect = new CONNECT().decode(frame);
            LOG.info("Unmarshalled: {}", connect);
            assertFalse(connect.cleanSession());
            assertEquals("user", connect.userName().toString());
            assertEquals("pass", connect.password().toString());
            assertEquals("test", connect.clientId().toString());
        }
    }

    @Test
    public void testProcessInChunks() throws Exception {

        CONNECT connect = new CONNECT();
        connect.cleanSession(false);
        connect.clientId(new UTF8Buffer("test"));
        connect.userName(new UTF8Buffer("user"));
        connect.password(new UTF8Buffer("pass"));

        DataByteArrayOutputStream output = new DataByteArrayOutputStream();
        wireFormat.marshal(connect.encode(), output);
        Buffer marshalled = output.toBuffer();

        DataByteArrayInputStream input = new DataByteArrayInputStream(marshalled);

        int first = marshalled.length() / 2;
        int second = marshalled.length() - first;

        codec.parse(input, first);
        codec.parse(input, second);

        assertTrue(!frames.isEmpty());
        assertEquals(1, frames.size());

        connect = new CONNECT().decode(frames.get(0));
        LOG.info("Unmarshalled: {}", connect);
        assertFalse(connect.cleanSession());

        assertEquals("user", connect.userName().toString());
        assertEquals("pass", connect.password().toString());
        assertEquals("test", connect.clientId().toString());
    }

    @Test
    public void testProcessInBytes() throws Exception {

        CONNECT connect = new CONNECT();
        connect.cleanSession(false);
        connect.clientId(new UTF8Buffer("test"));
        connect.userName(new UTF8Buffer("user"));
        connect.password(new UTF8Buffer("pass"));

        DataByteArrayOutputStream output = new DataByteArrayOutputStream();
        wireFormat.marshal(connect.encode(), output);
        Buffer marshalled = output.toBuffer();

        DataByteArrayInputStream input = new DataByteArrayInputStream(marshalled);

        int size = marshalled.length();

        for (int i = 0; i < size; ++i) {
            codec.parse(input, 1);
        }

        assertTrue(!frames.isEmpty());
        assertEquals(1, frames.size());

        connect = new CONNECT().decode(frames.get(0));
        LOG.info("Unmarshalled: {}", connect);
        assertFalse(connect.cleanSession());

        assertEquals("user", connect.userName().toString());
        assertEquals("pass", connect.password().toString());
        assertEquals("test", connect.clientId().toString());
    }

    @Test
    public void testMessageDecoding() throws Exception {

        byte[] CONTENTS = new byte[MESSAGE_SIZE];
        for (int i = 0; i < MESSAGE_SIZE; i++) {
            CONTENTS[i] = 'a';
        }

        PUBLISH publish = new PUBLISH();

        publish.dup(false);
        publish.messageId((short) 127);
        publish.qos(QoS.AT_LEAST_ONCE);
        publish.payload(new Buffer(CONTENTS));
        publish.topicName(new UTF8Buffer("TOPIC"));

        DataByteArrayOutputStream output = new DataByteArrayOutputStream();
        wireFormat.marshal(publish.encode(), output);
        Buffer marshalled = output.toBuffer();

        DataByteArrayInputStream input = new DataByteArrayInputStream(marshalled);
        codec.parse(input, marshalled.length());

        assertTrue(!frames.isEmpty());
        assertEquals(1, frames.size());

        publish = new PUBLISH().decode(frames.get(0));
        assertFalse(publish.dup());
        assertEquals(MESSAGE_SIZE, publish.payload().length());
    }

    @Test
    public void testMessageDecodingPerformance() throws Exception {

        byte[] CONTENTS = new byte[MESSAGE_SIZE];
        for (int i = 0; i < MESSAGE_SIZE; i++) {
            CONTENTS[i] = 'a';
        }

        PUBLISH publish = new PUBLISH();

        publish.dup(false);
        publish.messageId((short) 127);
        publish.qos(QoS.AT_LEAST_ONCE);
        publish.payload(new Buffer(CONTENTS));
        publish.topicName(new UTF8Buffer("TOPIC"));

        DataByteArrayOutputStream output = new DataByteArrayOutputStream();
        wireFormat.marshal(publish.encode(), output);
        Buffer marshalled = output.toBuffer();

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < ITERATIONS; ++i) {
            DataByteArrayInputStream input = new DataByteArrayInputStream(marshalled);
            codec.parse(input, marshalled.length());

            assertTrue(!frames.isEmpty());
            publish = new PUBLISH().decode(frames.get(0));
            frames.clear();
        }

        long duration = System.currentTimeMillis() - startTime;

        LOG.info("Total time to process: {}", TimeUnit.MILLISECONDS.toSeconds(duration));
    }
}
