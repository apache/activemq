/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.https;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsTopicSendReceiveTest;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class HttpsWantClientAuthSendAndReceiveTest extends JmsTopicSendReceiveTest {

    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    public static final String TRUST_KEYSTORE = "src/test/resources/client.keystore";
    public static final String KEYSTORE = "src/test/resources/server.keystore";


    private BrokerService broker;



    // set up broker with https first...
    @Override
    protected void setUp() throws Exception {
        broker = BrokerFactory.createBroker("xbean:activemq-https-want-client-auth.xml");
        broker.setPersistent(false);
        broker.start();
        broker.waitUntilStarted();

        // these are used for the client side... for the server side, the SSL context
        // will be configured through the <sslContext> spring beans
        System.setProperty("javax.net.ssl.trustStore", TRUST_KEYSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", KEYSTORE_TYPE);
//        System.setProperty("javax.net.ssl.keyStore", KEYSTORE);
//        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);
//        System.setProperty("javax.net.ssl.keyStoreType", KEYSTORE_TYPE);
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        if (broker != null) {
            broker.stop();
        }
    }

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("https://localhost:8161");
    }
}
