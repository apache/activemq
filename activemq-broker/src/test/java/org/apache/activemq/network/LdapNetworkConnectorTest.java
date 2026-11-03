/*
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
package org.apache.activemq.network;


import static org.apache.activemq.util.TransportValidationUtils.DENIED_TRANSPORT_SCHEMES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;

import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchResult;
import org.apache.activemq.broker.BrokerService;
import org.junit.Test;

public class LdapNetworkConnectorTest {

    @Test
    public void testValidation() throws Exception {
        LdapNetworkConnector connector = new LdapNetworkConnector();
        connector.setBrokerService(new BrokerService());

        for (String deniedScheme : DENIED_TRANSPORT_SCHEMES) {
            try {
                connector.addConnector(newSearchResult(deniedScheme, "localhost"));
                fail("Should have failed trying to add connector with scheme: " + deniedScheme);
            } catch (IllegalArgumentException e) {
                assertEquals("Transport scheme '" + deniedScheme + "' is not allowed", e.getMessage());
            }
        }
    }

    @Test
    public void testCompositeHostRejected() throws Exception {
        LdapNetworkConnector connector = new LdapNetworkConnector();

        // A host attribute that itself contains a composite URI is not a valid host value
        // and must be rejected before the URI is assembled.
        for (String scheme : DENIED_TRANSPORT_SCHEMES) {
            try {
                connector.toURI(newSearchResult("tcp", "static:(tcp://localhost," +
                                scheme + "://localhost)"));
                fail("Should have failed with host containing: " + scheme);
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage().contains("iphostnumber"));
            }
        }
    }

    @Test
    public void testInvalidHostRejected() throws Exception {
        LdapNetworkConnector connector = new LdapNetworkConnector();

        String[] invalidHosts = {
            "host.example.com)?x=y",
            "localhost,tcp://other",
            "localhost)",
            "localhost?soTimeout=1",
            "local host",
        };
        for (String host : invalidHosts) {
            try {
                connector.toURI(newSearchResult("tcp", host));
                fail("expected rejection for host [" + host + "]");
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage().contains("iphostnumber"));
            }
        }
    }

    @Test
    public void testInvalidProtocolRejected() throws Exception {
        LdapNetworkConnector connector = new LdapNetworkConnector();

        try {
            connector.toURI(newSearchResult("tcp://x:1)?a=b&(ssl", "localhost"));
            fail("expected rejection for invalid protocol");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("ipserviceprotocol"));
        }
    }

    @Test
    public void testInvalidPortRejected() throws Exception {
        LdapNetworkConnector connector = new LdapNetworkConnector();

        try {
            connector.toURI(newSearchResult("tcp", "localhost", "61616)?x=y"));
            fail("expected rejection for invalid port");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("ipserviceport"));
        }
    }

    @Test
    public void testValidComponentsAccepted() throws Exception {
        LdapNetworkConnector connector = new LdapNetworkConnector();

        assertEquals("static:(tcp://broker.example.com:61616)",
                connector.toURI(newSearchResult("tcp", "broker.example.com", "61616")).toString());
        assertEquals("static:(nio+ssl://10.0.0.1:61617)",
                connector.toURI(newSearchResult("nio+ssl", "10.0.0.1", "61617")).toString());
        assertEquals("static:(ssl://[::1]:61618)",
                connector.toURI(newSearchResult("ssl", "[::1]", "61618")).toString());
    }

    private SearchResult newSearchResult(String protocol, String host) {
        return newSearchResult(protocol, host, "0");
    }

    private SearchResult newSearchResult(String protocol, String host, String port) {
        BasicAttributes attrs = new BasicAttributes();
        attrs.put("ipserviceprotocol", protocol);
        attrs.put("iphostnumber", host);
        attrs.put("ipserviceport", port);
        SearchResult result = new SearchResult("name", null, attrs);
        result.setNameInNamespace("fullname");
        return result;
    }

}
