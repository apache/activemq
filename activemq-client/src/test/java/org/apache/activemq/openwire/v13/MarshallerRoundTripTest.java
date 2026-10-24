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
package org.apache.activemq.openwire.v13;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SharedConsumerInfo;
import org.apache.activemq.command.SharedSubscriptionInfo;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.openwire.OpenWireFormat;
import org.junit.Before;
import org.junit.Test;

public class MarshallerRoundTripTest {

    private OpenWireFormat wireFormat;

    @Before
    public void setUp() {
        wireFormat = new OpenWireFormat();
        wireFormat.setVersion(13);
    }

    @Test
    public void testVersionThirteenLoads() {
        assertEquals(13, wireFormat.getVersion());
    }

    @Test
    public void testSharedConsumerInfoRoundTrip() throws Exception {
        SharedConsumerInfo original = new SharedConsumerInfo();
        original.setConsumerId(new ConsumerId(new SessionId(new ConnectionId("conn1"), 1), 1));
        original.setDestination(new ActiveMQTopic("test.topic"));
        original.setSubscriptionName("mySub");
        original.setShared(true);
        original.setDurable(true);
        original.setPrefetchSize(100);

        byte[] bytes = marshal(original);
        Object result = unmarshal(bytes);

        assertTrue("Should unmarshal as SharedConsumerInfo", result instanceof SharedConsumerInfo);
        SharedConsumerInfo restored = (SharedConsumerInfo) result;
        assertTrue(restored.isShared());
        assertTrue(restored.isDurable());
        assertEquals("mySub", restored.getSubscriptionName());
        assertEquals(100, restored.getPrefetchSize());
        assertEquals(new ActiveMQTopic("test.topic"), restored.getDestination());
    }

    @Test
    public void testSharedConsumerInfoNonSharedRoundTrip() throws Exception {
        SharedConsumerInfo original = new SharedConsumerInfo();
        original.setConsumerId(new ConsumerId(new SessionId(new ConnectionId("conn1"), 1), 2));
        original.setDestination(new ActiveMQTopic("test.topic"));
        original.setShared(false);
        original.setDurable(false);

        byte[] bytes = marshal(original);
        Object result = unmarshal(bytes);

        assertTrue(result instanceof SharedConsumerInfo);
        SharedConsumerInfo restored = (SharedConsumerInfo) result;
        assertFalse(restored.isShared());
        assertFalse(restored.isDurable());
    }

    @Test
    public void testSharedSubscriptionInfoRoundTrip() throws Exception {
        SharedSubscriptionInfo original = new SharedSubscriptionInfo();
        original.setClientId("client1");
        original.setSubscriptionName("mySub");
        original.setSelector("color = 'blue'");
        original.setDestination(new ActiveMQTopic("test.topic"));
        original.setNoLocal(false);
        original.setShared(true);

        byte[] bytes = marshal(original);
        Object result = unmarshal(bytes);

        assertTrue("Should unmarshal as SharedSubscriptionInfo",
                result instanceof SharedSubscriptionInfo);
        SharedSubscriptionInfo restored = (SharedSubscriptionInfo) result;
        assertTrue(restored.isShared());
        assertEquals("client1", restored.getClientId());
        assertEquals("mySub", restored.getSubscriptionName());
        assertEquals("color = 'blue'", restored.getSelector());
    }

    @Test
    public void testSharedSubscriptionInfoNullClientId() throws Exception {
        SharedSubscriptionInfo original = new SharedSubscriptionInfo();
        original.setSubscriptionName("sharedSub");
        original.setDestination(new ActiveMQTopic("test.topic"));
        original.setShared(true);

        byte[] bytes = marshal(original);
        Object result = unmarshal(bytes);

        assertTrue(result instanceof SharedSubscriptionInfo);
        SharedSubscriptionInfo restored = (SharedSubscriptionInfo) result;
        assertNull(restored.getClientId());
        assertTrue(restored.isShared());
    }

    @Test
    public void testPlainConsumerInfoMarshalledAsNonShared() throws Exception {
        ConsumerInfo original = new ConsumerInfo();
        original.setConsumerId(new ConsumerId(new SessionId(new ConnectionId("conn1"), 1), 3));
        original.setDestination(new ActiveMQTopic("test.topic"));
        original.setSubscriptionName("durableSub");

        byte[] bytes = marshal(original);
        Object result = unmarshal(bytes);

        assertTrue(result instanceof SharedConsumerInfo);
        SharedConsumerInfo restored = (SharedConsumerInfo) result;
        assertFalse(restored.isShared());
        assertFalse(restored.isDurable());
        assertEquals("durableSub", restored.getSubscriptionName());
    }

    @Test
    public void testMarshallerFactoryReplacesOnlyTwoMarshallers() {
        org.apache.activemq.openwire.DataStreamMarshaller[] map =
                MarshallerFactory.createMarshallerMap(wireFormat);

        assertTrue(map[ConsumerInfo.DATA_STRUCTURE_TYPE & 0xFF]
                instanceof ConsumerInfoMarshaller);
        assertTrue(map[SubscriptionInfo.DATA_STRUCTURE_TYPE & 0xFF]
                instanceof SubscriptionInfoMarshaller);
    }

    /**
     * A v12 peer must see an ordinary {@link ConsumerInfo} with none of the
     * shared fields, so a broker left at the default store version behaves
     * exactly as it did before v13 existed.
     */
    @Test
    public void testSharedFlagsAreNotWrittenAtV12() throws Exception {
        wireFormat.setVersion(12);

        SharedConsumerInfo original = new SharedConsumerInfo();
        original.setConsumerId(new ConsumerId(new SessionId(new ConnectionId("conn1"), 1), 4));
        original.setDestination(new ActiveMQTopic("test.topic"));
        original.setSubscriptionName("mySub");
        original.setShared(true);
        original.setDurable(true);

        Object result = unmarshal(marshal(original));

        assertFalse("v12 must not produce a SharedConsumerInfo",
                result instanceof SharedConsumerInfo);
        assertTrue(result instanceof ConsumerInfo);
        assertEquals("mySub", ((ConsumerInfo) result).getSubscriptionName());
    }

    @Test
    public void testSharedSubscriptionFlagIsNotWrittenAtV12() throws Exception {
        wireFormat.setVersion(12);

        SharedSubscriptionInfo original = new SharedSubscriptionInfo("client1", "mySub");
        original.setDestination(new ActiveMQTopic("test.topic"));
        original.setShared(true);

        Object result = unmarshal(marshal(original));

        assertFalse("v12 must not produce a SharedSubscriptionInfo",
                result instanceof SharedSubscriptionInfo);
        assertTrue(result instanceof SubscriptionInfo);
        assertEquals("mySub", ((SubscriptionInfo) result).getSubscriptionName());
    }

    private byte[] marshal(Object obj) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        wireFormat.marshal(obj, out);
        out.flush();
        return baos.toByteArray();
    }

    private Object unmarshal(byte[] bytes) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);
        return wireFormat.unmarshal(in);
    }
}
