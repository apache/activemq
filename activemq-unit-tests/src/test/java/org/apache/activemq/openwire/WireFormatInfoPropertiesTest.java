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
package org.apache.activemq.openwire;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQConnectionMetaData;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.transport.DefaultTransportListener;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WireFormatInfoPropertiesTest {

    static final Logger LOG = LoggerFactory.getLogger(WireFormatInfoPropertiesTest.class);

    protected BrokerService master;
    protected String brokerUri;

    @Test
    public void testClientProperties() throws Exception{
        BrokerService service = createBrokerService();
        try {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(new URI(brokerUri));
            ActiveMQConnection conn = (ActiveMQConnection)factory.createConnection();
            final AtomicReference<WireFormatInfo> clientWf = new AtomicReference<WireFormatInfo>();
            conn.addTransportListener(new DefaultTransportListener() {
                @Override
                public void onCommand(Object command) {
                    if (command instanceof WireFormatInfo) {
                        clientWf.set((WireFormatInfo)command);
                    }
                }
            });
            conn.start();
            if (clientWf.get() == null) {
                fail("Wire format info is null");
            }
            assertTrue(clientWf.get().getProperties().containsKey("ProviderName"));
            assertTrue(clientWf.get().getProperties().containsKey("ProviderVersion"));
            assertTrue(clientWf.get().getProperties().containsKey("PlatformDetails"));
            assertTrue(clientWf.get().getProviderName().equals(ActiveMQConnectionMetaData.PROVIDER_NAME));
            assertTrue(clientWf.get().getPlatformDetails().equals(ActiveMQConnectionMetaData.PLATFORM_DETAILS));
        } finally {
            stopBroker(service);
        }
    }

    @Test
    public void testMarshalClientProperties() throws IOException {
        // marshal object
        OpenWireFormatFactory factory = new OpenWireFormatFactory();
        OpenWireFormat wf = (OpenWireFormat)factory.createWireFormat();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream ds = new DataOutputStream(buffer);
        WireFormatInfo orig = wf.getPreferedWireFormatInfo();
        wf.marshal(orig, ds);
        ds.close();

        // unmarshal object and check that the properties are present.
        ByteArrayInputStream in = new ByteArrayInputStream(buffer.toByteArray());
        DataInputStream dis = new DataInputStream(in);
        Object actual = wf.unmarshal(dis);

        if (!(actual instanceof WireFormatInfo)) {
            fail("Unknown type");
        }
        WireFormatInfo result = (WireFormatInfo)actual;
        assertTrue(result.getProviderName().equals(orig.getProviderName()));
        // the version won't be valid until runtime
        assertTrue(result.getProviderVersion() == null || result.getProviderVersion().equals(orig.getProviderVersion()));
        assertTrue(result.getPlatformDetails().equals(orig.getPlatformDetails()));
    }

    private BrokerService createBrokerService() throws Exception {
        BrokerService service = new BrokerService();
        TransportConnector connector = service.addConnector("tcp://localhost:0");
        brokerUri = connector.getPublishableConnectString();
        service.setPersistent(false);
        service.setUseJmx(false);
        service.setBrokerName("Master");
        service.start();
        service.waitUntilStarted();
        return service;
    }

    private void stopBroker(BrokerService service) throws Exception {
        if (service != null) {
            service.stop();
            service.waitUntilStopped();
        }
    }

}
