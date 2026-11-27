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
package org.apache.activemq.transport.https;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.transport.SslMode;
import org.apache.activemq.transport.TransportServerSupport;
import org.apache.activemq.transport.wss.WSSTransportServer;
import org.junit.Test;

public class HttpsSslModeTest {

    @Test
    public void testHttps() throws Exception {
        assertSslMode(new HttpsTransportServer(new URI("https://localhost:0"), new HttpsTransportFactory(), null), "");
    }

    @Test
    public void testWss() throws Exception {
        assertSslMode(new WSSTransportServer(new URI("wss://localhost:0"), null), "transport.");
    }

    private static void assertSslMode(TransportServerSupport server, String prefix) {
        assertEquals(SslMode.NONE, server.getSslMode());

        Map<String, Object> options = new HashMap<>();
        options.put(prefix + "wantClientAuth", "true");
        server.setTransportOption(options);
        assertEquals(SslMode.WANT_CLIENT_AUTH, server.getSslMode());

        options = new HashMap<>();
        options.put(prefix + "needClientAuth", "true");
        server.setTransportOption(options);
        assertEquals(SslMode.NEED_CLIENT_AUTH, server.getSslMode());
    }
}
