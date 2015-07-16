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
package org.apache.activemq.transport.auto;

import java.net.URI;
import java.net.URISyntaxException;
import junit.framework.Test;
import junit.textui.TestRunner;
import org.apache.activemq.transport.TransportBrokerTestSupport;

public class AutoSslTransportBrokerTest extends TransportBrokerTestSupport {

    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/server.keystore";
    public static final String TRUST_KEYSTORE = "src/test/resources/client.keystore";

    @Override
    protected String getBindLocation() {
        return "auto+ssl://localhost:0?transport.soWriteTimeout=20000";
    }

    @Override
    protected URI getBindURI() throws URISyntaxException {
        return new URI("auto+ssl://localhost:0?soWriteTimeout=20000");
    }

    @Override
    protected void setUp() throws Exception {
        System.setProperty("javax.net.ssl.trustStore", TRUST_KEYSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStore", SERVER_KEYSTORE);
        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.keyStoreType", KEYSTORE_TYPE);
        //System.setProperty("javax.net.debug", "ssl,handshake,data,trustmanager");

        maxWait = 10000;
        super.setUp();
    }

    public static Test suite() {
        return suite(AutoSslTransportBrokerTest.class);
    }

    public static void main(String[] args) {
        TestRunner.run(suite());
    }

}
