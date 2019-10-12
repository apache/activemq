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
package org.apache.activemq.transport.http;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.test.JmsTopicSendReceiveWithTwoConnectionsTest;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.http.openwire.AssertingTransportFactory;
import org.apache.activemq.transport.http.openwire.CustomHttpTransportFactory;
import org.apache.activemq.transport.http.openwire.SpyMarshaller;
import org.apache.activemq.transport.http.marshallers.HttpWireFormatMarshaller;

import java.util.LinkedList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

/**
 * 
 */
public class HttpOpenWireSendAndReceiveTest extends JmsTopicSendReceiveWithTwoConnectionsTest {
    private static final String CUSTOM_HTTP_PROTOCOL = "http";
    private static final String WIRE_FORMAT_OPENWIRE = "default";
    private final AssertingTransportFactory clientTransportFactory = new AssertingTransportFactory(WIRE_FORMAT_OPENWIRE, HttpWireFormatMarshaller.class);

    protected BrokerService broker;

    @Override
    protected void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker();
            broker.start();
        }
        super.setUp();
        WaitForJettyListener.waitForJettySocketToAccept(getBrokerURL());
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        if (broker != null) {
            broker.stop();
        }
    }

    @Override
    public void testSendReceive() throws Exception
    {
        super.testSendReceive();
        final LinkedList<SpyMarshaller> usedMarshallers = clientTransportFactory.getSpyMarshallers();
        assertThat(usedMarshallers.size(), equalTo(2));
        final SpyMarshaller marshaller1 = usedMarshallers.pop();
        final SpyMarshaller marshaller2 = usedMarshallers.pop();

        assertThat(marshaller1.getMarshallCallsCnt(), not(equalTo(0)));
        assertThat(marshaller1.getUnmarshallCallsCnt(), not(equalTo(0)));

        assertThat(marshaller1.getMarshallCallsCnt(), equalTo(marshaller2.getMarshallCallsCnt()));
        assertThat(marshaller1.getUnmarshallCallsCnt(), equalTo(marshaller2.getUnmarshallCallsCnt()));
    }

    protected String getBrokerURL() {
        return "http://localhost:8161";
    }

    protected BrokerService createBroker() throws Exception {
        final BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        addConnector(broker, getBrokerURL());
        return broker;
    }

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory() {
        TransportFactory.registerTransportFactory(CUSTOM_HTTP_PROTOCOL, clientTransportFactory);
        return new ActiveMQConnectionFactory(getBrokerURL());
    }

    private static void addConnector(final BrokerService brokerService, final String brokerURL) throws Exception {
        TransportFactory.registerTransportFactory(CUSTOM_HTTP_PROTOCOL, new CustomHttpTransportFactory());

        brokerService.addConnector(brokerURL);
    }
}
