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

package org.apache.activemq.broker;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class PublishedAddressPolicyTest {
    PublishedAddressPolicy underTest = new PublishedAddressPolicy();

    final AtomicReference<URI> uriAtomicReference = new AtomicReference<>();

    final TransportConnector dummyTransportConnector = new TransportConnector() {
        @Override
        public URI getConnectUri() throws IOException, URISyntaxException {
            return uriAtomicReference.get();
        }
    };

    @Before
    public void setTransport() throws Exception {
        URI ok = new URI("tcp://bob:88");
        uriAtomicReference.set(ok);
    }

    @Test
    public void testDefaultReturnsHost() throws Exception {
        assertTrue("contains bob", underTest.getPublishableConnectString(dummyTransportConnector).contains("bob"));
    }

    @Test
    public void testHostMap() throws Exception {
        HashMap<String, String> hostMap = new HashMap<>();
        hostMap.put("bob", "pat");
        underTest.setHostMapping(hostMap);
        assertTrue("contains pat", underTest.getPublishableConnectString(dummyTransportConnector).contains("pat"));
    }

    @Test
    public void testPortMap() throws Exception {
        Map<Integer, Integer> portMap = new HashMap<Integer, Integer>();
        portMap.put(88, 77);
        underTest.setPortMapping(portMap);
        assertTrue("contains 77", underTest.getPublishableConnectString(dummyTransportConnector).contains("77"));
    }
}