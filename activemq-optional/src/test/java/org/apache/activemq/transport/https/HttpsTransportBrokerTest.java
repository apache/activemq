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

import junit.framework.Test;
import junit.textui.TestRunner;
import org.apache.activemq.transport.http.HttpTransportBrokerTest;

public class HttpsTransportBrokerTest extends HttpTransportBrokerTest {

    protected String getBindLocation() {
        return "https://localhost:8161";
    }

    protected void setUp() throws Exception {
        System.setProperty("javax.net.ssl.trustStore", "src/test/resources/client.keystore");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStoreType", "jks");
        System.setProperty("javax.net.ssl.keyStore", "src/test/resources/server.keystore");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.keyStoreType", "jks");
        //System.setProperty("javax.net.debug", "ssl,handshake,data,trustmanager");
        super.setUp();

        Thread.sleep(2000);
        Thread.yield();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        // Give the jetty server more time to shutdown before starting another one
        Thread.sleep(2000);
    }
    
    public static Test suite() {
        return suite(HttpsTransportBrokerTest.class);
    }

    public static void main(String[] args) {
        TestRunner.run(suite());
    }

}
