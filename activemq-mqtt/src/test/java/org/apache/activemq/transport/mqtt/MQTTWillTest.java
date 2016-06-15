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

import org.fusesource.mqtt.client.*;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MQTTWillTest extends MQTTTestSupport {

    @Test(timeout = 60 * 1000)
    public void testWillMessage() throws Exception {
        BlockingConnection conn1 = null;
        BlockingConnection conn2 = null;
        try {
            MQTT mqtt1 = createMQTTConnection("client1", false);
            mqtt1.setWillMessage("last will");
            mqtt1.setWillQos(QoS.AT_LEAST_ONCE);
            mqtt1.setWillTopic("wills");

            conn1 = mqtt1.blockingConnection();
            conn1.connect();

            MQTT mqtt2 = createMQTTConnection("client2", false);
            conn2 = mqtt2.blockingConnection();
            conn2.connect();
            conn2.subscribe(new Topic[]{new Topic("#", QoS.AT_LEAST_ONCE)});

            conn1.publish("test", "hello world".getBytes(), QoS.AT_LEAST_ONCE, false);

            Message msg = conn2.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            assertEquals("hello world", new String(msg.getPayload()));
            assertEquals("test", msg.getTopic());

            conn1.kill();

            msg = conn2.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            assertEquals("last will", new String(msg.getPayload()));
            assertEquals("wills", msg.getTopic());
        } finally {
           if (conn1 != null) {
               conn1.disconnect();
           }
           if (conn2 != null) {
               conn2.disconnect();
           }
        }
    }

    @Test(timeout = 60 * 1000)
    public void testRetainWillMessage() throws Exception {
        BlockingConnection conn1 = null;
        BlockingConnection conn2 = null;
        try {
            MQTT mqtt1 = createMQTTConnection("client1", false);
            mqtt1.setWillMessage("last will");
            mqtt1.setWillQos(QoS.AT_LEAST_ONCE);
            mqtt1.setWillTopic("wills");
            mqtt1.setWillRetain(true);

            conn1 = mqtt1.blockingConnection();
            conn1.connect();

            MQTT mqtt2 = createMQTTConnection("client2", false);
            conn2 = mqtt2.blockingConnection();
            conn2.connect();
            conn2.subscribe(new Topic[]{new Topic("#", QoS.AT_MOST_ONCE)});

            conn1.publish("test", "hello world".getBytes(), QoS.AT_LEAST_ONCE, false);

            Message msg = conn2.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            assertEquals("hello world", new String(msg.getPayload()));
            assertEquals("test", msg.getTopic());
            msg.ack();

            conn2.disconnect();

            Thread.sleep(1000);
            conn1.kill();
            Thread.sleep(1000);

            conn2 = mqtt2.blockingConnection();
            conn2.connect();
            conn2.subscribe(new Topic[]{new Topic("#", QoS.AT_MOST_ONCE)});

            msg = conn2.receive(5, TimeUnit.SECONDS);
            System.out.println(msg.getTopic() + " " + new String(msg.getPayload()));
            assertNotNull(msg);
            assertEquals("last will", new String(msg.getPayload()));
            assertEquals("wills", msg.getTopic());
        } finally {
           if (conn1 != null) {
               conn1.disconnect();
           }
           if (conn2 != null) {
               conn2.disconnect();
           }
        }
    }

}
