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

package org.apache.activemq.transport.wss;

import org.apache.activemq.transport.http.HttpTraceTestSupport;
import org.apache.activemq.transport.ws.WSTransportHttpTraceTest;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class WSSTransportHttpTraceTest extends WSTransportHttpTraceTest {

    public WSSTransportHttpTraceTest(String enableTraceParam, int expectedStatus) {
        super(enableTraceParam, expectedStatus);
    }

    /**
     * This tests whether the TRACE method is enabled or not
     * @throws Exception
     */
    @Override
    @Test(timeout=10000)
    public void testHttpTraceEnabled() throws Exception {
        SslContextFactory factory = new SslContextFactory();
        factory.setSslContext(broker.getSslContext().getSSLContext());

        HttpTraceTestSupport.testHttpTraceEnabled("https://127.0.0.1:61623", expectedStatus, factory);
    }

    @Override
    protected String getWSConnectorURI() {
        String uri = "wss://127.0.0.1:61623?websocket.maxTextMessageSize=99999&transport.maxIdleTime=1001";
        uri = enableTraceParam != null ? uri + "&" + enableTraceParam : uri;
        return uri;
    }
}
