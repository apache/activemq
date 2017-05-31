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
package org.apache.activemq.transport.ws;

import static org.junit.Assert.assertEquals;

import org.apache.activemq.transport.ws.jetty9.MQTTSocket;
import org.apache.activemq.transport.ws.jetty9.StompSocket;
import org.junit.Test;

public class SocketTest {

    @Test
    public void testStompSocketRemoteAddress() {

        StompSocket stompSocketJetty8 = new StompSocket("ws://localhost:8080");

        assertEquals("ws://localhost:8080", stompSocketJetty8.getRemoteAddress());

        org.apache.activemq.transport.ws.jetty9.StompSocket stompSocketJetty9 =
                new org.apache.activemq.transport.ws.jetty9.StompSocket("ws://localhost:8080");

        assertEquals("ws://localhost:8080", stompSocketJetty9.getRemoteAddress());
    }

    @Test
    public void testMqttSocketRemoteAddress() {

        MQTTSocket mqttSocketJetty8 = new MQTTSocket("ws://localhost:8080");

        assertEquals("ws://localhost:8080", mqttSocketJetty8.getRemoteAddress());

        MQTTSocket mqttSocketJetty9 = new MQTTSocket("ws://localhost:8080");

        assertEquals("ws://localhost:8080", mqttSocketJetty9.getRemoteAddress());
    }
}
