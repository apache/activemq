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
package org.apache.activemq.transport.mqtt.auto;

import static org.junit.Assert.assertTrue;

import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.transport.mqtt.MQTTTestSupport;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class MQTTAutoSslAuthTest extends MQTTTestSupport  {

    private final String protocol;
    private boolean hasCertificate = false;

    @Parameters(name="scheme={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"auto+nio+ssl"},
                {"auto+ssl"}
            });
    }

    /**
     * @param isNio
     */
    public MQTTAutoSslAuthTest(String protocol) {
        this.protocol = protocol;
        protocolConfig = "transport.needClientAuth=true";
    }

    @Override
    public boolean isUseSSL() {
        return true;
    }

    @Override
    public String getProtocolScheme() {
        return protocol;
    }

    @Override
    protected void createPlugins(List<BrokerPlugin> plugins) throws Exception {
        super.createPlugins(plugins);
        plugins.add(new BrokerPlugin() {

            @Override
            public Broker installPlugin(Broker broker) throws Exception {
                return new BrokerFilter(broker) {

                    @Override
                    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
                        super.addConnection(context, info);
                        //The second time should contain the certificate
                        hasCertificate = info.getTransportContext() instanceof X509Certificate[];
                    }
                };
            }
        });
    }

    @Test(timeout = 60 * 1000)
    public void testMQTT311Connection() throws Exception {
        MQTT mqtt = createMQTTConnection();
        mqtt.setClientId("foo");
        mqtt.setVersion("3.1.1");
        final BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        connection.disconnect();

        assertTrue(hasCertificate);
    }
}
