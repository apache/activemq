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

import java.util.Collection;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class HttpTransportHttpTraceTest {

    private BrokerService broker;
    private String uri;

    protected String enableTraceParam;
    private int expectedStatus;


    @Before
    public void setUp() throws Exception {
        additionalConfig();

        broker = new BrokerService();
        TransportConnector connector = broker.addConnector(getConnectorUri());
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.deleteAllMessages();
        broker.addConnector(connector);
        broker.start();

        uri = connector.getPublishableConnectString();
    }

    protected String getConnectorUri() {
        return "http://localhost:0?" + enableTraceParam;
    }

    protected void additionalConfig() {

    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }

    @Parameters
    public static Collection<Object[]> data() {
        return HttpTraceTestSupport.getTestParameters();
    }

    public HttpTransportHttpTraceTest(final String enableTraceParam, final int expectedStatus) {
        this.enableTraceParam = enableTraceParam;
        this.expectedStatus = expectedStatus;
    }

    /**
     * This tests whether the TRACE method is enabled or not
     * @throws Exception
     */
    @Test(timeout=10000)
    public void testHttpTraceEnabled() throws Exception {
        HttpTraceTestSupport.testHttpTraceEnabled(uri, expectedStatus);
    }

}
