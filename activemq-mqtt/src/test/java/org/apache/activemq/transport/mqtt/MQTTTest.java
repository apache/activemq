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

import org.apache.activemq.util.Wait;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.junit.Test;
import static org.junit.Assert.assertTrue;


public class MQTTTest extends AbstractMQTTTest {

    @Test
    public void testPingKeepsInactivityMonitorAlive() throws Exception {
        addMQTTConnector();
        brokerService.start();
        MQTT mqtt = createMQTTConnection();
        mqtt.setKeepAlive((short)2);
        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        assertTrue("KeepAlive didn't work properly", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return connection.isConnected();
            }
        }));

        connection.disconnect();
    }

    @Test
    public void testTurnOffInactivityMonitor()throws Exception{
        addMQTTConnector("?transport.useInactivityMonitor=false");
        brokerService.start();
        MQTT mqtt = createMQTTConnection();
        mqtt.setKeepAlive((short)2);
        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        assertTrue("KeepAlive didn't work properly", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return connection.isConnected();
            }
        }));

        connection.disconnect();
    }


    @Test
    public void testDefaultKeepAliveWhenClientSpecifiesZero() throws Exception {
        // default keep alive in milliseconds
        addMQTTConnector("?transport.defaultKeepAlive=2000");
        brokerService.start();
        MQTT mqtt = createMQTTConnection();
        mqtt.setKeepAlive((short)0);
        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        assertTrue("KeepAlive didn't work properly", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return connection.isConnected();
            }
        }));
    }


    protected String getProtocolScheme() {
        return "mqtt";
    }

    protected void addMQTTConnector() throws Exception {
        addMQTTConnector("");
    }

    protected void addMQTTConnector(String config) throws Exception {
        mqttConnector= brokerService.addConnector(getProtocolScheme()+"://localhost:0" + config);
    }

    @Override
    protected MQTTClientProvider getMQTTClientProvider() {
        return new FuseMQQTTClientProvider();
    }

    protected MQTT createMQTTConnection() throws Exception {
        MQTT mqtt = new MQTT();
        mqtt.setHost("localhost", mqttConnector.getConnectUri().getPort());
        // shut off connect retry
        mqtt.setConnectAttemptsMax(0);
        mqtt.setReconnectAttemptsMax(0);
        return mqtt;
    }
}