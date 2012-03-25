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

import java.util.Vector;

import org.apache.activemq.broker.BrokerService;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// https://issues.apache.org/jira/browse/AMQ-3393
public class MQTTConnectTest {
    private static final Logger LOG = LoggerFactory.getLogger(MQTTConnectTest.class);
    BrokerService brokerService;
    Vector<Throwable> exceptions = new Vector<Throwable>();

    @Before
    public void startBroker() throws Exception {
        exceptions.clear();
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setAdvisorySupport(false);
    }

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    @Test
    public void testConnect() throws Exception {

        brokerService.addConnector("mqtt://localhost:1883");
        brokerService.start();
        MQTT mqtt = new MQTT();
        mqtt.setHost("localhost",1883);
        BlockingConnection connection = mqtt.blockingConnection();

        connection.connect();
        Thread.sleep(1000);
        connection.disconnect();
    }

    
}