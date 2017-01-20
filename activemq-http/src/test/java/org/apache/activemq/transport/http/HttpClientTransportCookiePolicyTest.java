/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.http;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.activemq.transport.util.TextWireFormat;
import org.apache.http.client.HttpClient;
import org.apache.http.client.params.HttpClientParams;
import org.junit.Before;
import org.junit.Test;

/**
 * Test that {@link HttpClientTransport} sets a broad-range compatibility
 * cookie policy.
 *
 * @see <a href="https://issues.apache.org/jira/browse/AMQ-6571">AMQ-6571: HttpClientTransport refuses to accept cookies using `Expires' header</a>
 */
@SuppressWarnings("deprecation")
public class HttpClientTransportCookiePolicyTest {

    private HttpClientTransport transport;


    /**
     * Create the transport so we can inspect it.
     * @throws URISyntaxException if something goes wrong.
     */
    @Before
    public void setUp() throws URISyntaxException {
        transport = new HttpClientTransport(mock(TextWireFormat.class), new URI("http://localhost:8080/test"));
    }


    /**
     * Create a new connection and check the connection properties.
     */
    @Test
    public void test() {
        HttpClient client = transport.createHttpClient();
        assertEquals("Cookie spec", org.apache.http.client.params.CookiePolicy.BROWSER_COMPATIBILITY, HttpClientParams.getCookiePolicy(client.getParams()));
    }
}
