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
package org.apache.activemq.transport.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpServletRequest;

import org.junit.Test;

public class HttpTransportUtilsTest {

    @Test
    public void testGenerateWsRemoteAddress() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getScheme()).thenReturn("http");
        when(request.getRemoteAddr()).thenReturn("localhost");
        when(request.getRemotePort()).thenReturn(8080);

        assertEquals("ws://localhost:8080", HttpTransportUtils.generateWsRemoteAddress(request));
    }

    @Test
    public void testGenerateWssRemoteAddress() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getScheme()).thenReturn("https");
        when(request.getRemoteAddr()).thenReturn("localhost");
        when(request.getRemotePort()).thenReturn(8443);

        assertEquals("wss://localhost:8443", HttpTransportUtils.generateWsRemoteAddress(request));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testNullHttpServleRequest() {
        HttpTransportUtils.generateWsRemoteAddress(null);
    }
}
