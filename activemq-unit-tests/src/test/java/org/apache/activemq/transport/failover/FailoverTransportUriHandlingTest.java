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
package org.apache.activemq.transport.failover;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collection;

import org.apache.activemq.transport.failover.FailoverTransport;
import org.junit.Test;

public class FailoverTransportUriHandlingTest {

    @Test
    public void testFailoverTransportAddWithInitialUnknown() throws Exception {
        FailoverTransport transport = new FailoverTransport();

        final String initialUri = "tcp://no.existing.hostname:61616";

        transport.add(false, initialUri);

        String[] uriArray = new String[] {"tcp://127.0.0.2:61616",
                                          "tcp://localhost:61616",
                                          "tcp://localhost:61617"};

        for(String uri : uriArray) {
            transport.add(false, uri);
        }

        Collection<URI> uris = getRegisteredUrlsFromPrivateField(transport);

        for(String uri : uriArray) {
            assertTrue("Collection should contain: " + uri, uris.contains(new URI(uri)));
        }
    }

    @Test
    public void testFailoverTransportAddWithInitialKnown() throws Exception {
        FailoverTransport transport = new FailoverTransport();

        final String initialUri = "tcp://localhost:61616";

        transport.add(false, initialUri);

        String[] uriArray = new String[] {"tcp://127.0.0.2:61616",
                                          "tcp://no.existing.hostname:61616",
                                          "tcp://localhost:61617"};

        for(String uri : uriArray) {
            transport.add(false, uri);
        }

        Collection<URI> uris = getRegisteredUrlsFromPrivateField(transport);

        for(String uri : uriArray) {
            assertTrue("Collection should contain: " + uri, uris.contains(new URI(uri)));
        }
    }

    @Test
    public void testFailoverTransportAddWithPreventsDups() throws Exception {
        FailoverTransport transport = new FailoverTransport();

        final String initialUri = "tcp://localhost:61616";

        transport.add(false, initialUri);

        String[] uriArray = new String[] {"tcp://127.0.0.2:61616",
                                          "tcp://localhost:61616",
                                          "tcp://no.existing.hostname:61616",
                                          "tcp://localhost:61617",
                                          "tcp://127.0.0.1:61616"};

        for(String uri : uriArray) {
            transport.add(false, uri);
        }

        Collection<URI> uris = getRegisteredUrlsFromPrivateField(transport);

        assertEquals(4, uris.size());

        // Ensure even the unknowns get checked.
        transport.add(false, "tcp://no.existing.hostname:61616");

        uris = getRegisteredUrlsFromPrivateField(transport);

        assertEquals(4, uris.size());
    }

    @Test
    public void testFailoverTransportAddArray() throws Exception {
        FailoverTransport transport = new FailoverTransport();

        final String initialUri = "tcp://no.existing.hostname:61616";

        transport.add(false, initialUri);

        URI[] uriArray = new URI[] {new URI("tcp://127.0.0.2:61616"),
                                    new URI("tcp://localhost:61616"),
                                    new URI("tcp://localhost:61617")};

        transport.add(false, uriArray);

        Collection<URI> uris = getRegisteredUrlsFromPrivateField(transport);

        for(URI uri : uriArray) {
            assertTrue("Collection should contain: " + uri, uris.contains(uri));
        }

        assertEquals(4, uris.size());

        // Ensure even the unknowns get checked.
        transport.add(false, "tcp://no.existing.hostname:61616");

        uris = getRegisteredUrlsFromPrivateField(transport);

        assertEquals(4, uris.size());

        transport.add(false, uriArray);

        assertEquals(4, uris.size());
    }

    @SuppressWarnings("unchecked")
    private Collection<URI> getRegisteredUrlsFromPrivateField(FailoverTransport failoverTransport) throws SecurityException, NoSuchFieldException, IllegalAccessException {
        Field urisField = failoverTransport.getClass().getDeclaredField("uris");
        urisField.setAccessible(true);
        return (Collection<URI>) urisField.get(failoverTransport);
    }
}
