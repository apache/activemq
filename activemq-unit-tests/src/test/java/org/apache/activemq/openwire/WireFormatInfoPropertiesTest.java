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

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQConnectionMetaData;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.WireFormatInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WireFormatInfoPropertiesTest {

    static final Logger LOG = LoggerFactory.getLogger(WireFormatInfoPropertiesTest.class);

    private BrokerService service;
    private String brokerUri;
    private TransportConnector connector;

    @Before
    public void before() throws Exception {
        service = new BrokerService();
        connector = service.addConnector("tcp://localhost:0");
        brokerUri = connector.getPublishableConnectString();
        service.setPersistent(false);
        service.setUseJmx(false);
        service.setBrokerName("Master");
        service.start();
        service.waitUntilStarted();
    }

    @After
    public void after() throws Exception {
        if (service != null) {
            service.stop();
            service.waitUntilStopped();
        }
    }

    @Test
    public void testClientPropertiesWithDefaultPlatformDetails() throws Exception{
        WireFormatInfo clientWf = testClientProperties(brokerUri);
        assertTrue(clientWf.getPlatformDetails().equals(ActiveMQConnectionMetaData.DEFAULT_PLATFORM_DETAILS));
    }

    @Test
    public void testClientPropertiesWithPlatformDetails() throws Exception{
        WireFormatInfo clientWf = testClientProperties(brokerUri + "?wireFormat.includePlatformDetails=true");
        assertTrue(clientWf.getPlatformDetails().equals(ActiveMQConnectionMetaData.PLATFORM_DETAILS));
    }

    private WireFormatInfo testClientProperties(String brokerUri) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(new URI(brokerUri));
        ActiveMQConnection conn = (ActiveMQConnection)factory.createConnection();
        conn.start();

        assertTrue(connector.getConnections().size() == 1);
        final WireFormatInfo clientWf = connector.getConnections().get(0).getRemoteWireFormatInfo();
        if (clientWf == null) {
            fail("Wire format info is null");
        }

        //verify properties that the client sends to the broker
        assertTrue(clientWf.getProperties().containsKey("ProviderName"));
        assertTrue(clientWf.getProperties().containsKey("ProviderVersion"));
        assertTrue(clientWf.getProperties().containsKey("PlatformDetails"));
        assertTrue(clientWf.getProviderName().equals(ActiveMQConnectionMetaData.PROVIDER_NAME));

        return clientWf;
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

}
