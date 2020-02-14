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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.JmsTopicSendReceiveTest;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.spring.SpringSslContext;

/**
 * Here we are using a TLS cert which does not have "localhost" as the CN. However we configure the client not to enable
 * hostname verification, and so the test passes
 */
public class HttpsClientSettingsHostnameVerificationDisabledTest extends JmsTopicSendReceiveTest {

    /**
     * 
     */
    private static final String URI_LOCATION = "https://localhost:8161";
    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    public static final String TRUST_KEYSTORE = "src/test/resources/server-somehost.keystore";
    public static final String SERVER_KEYSTORE = "src/test/resources/server-somehost.keystore";

    private BrokerService broker;

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.JmsSendReceiveTestSupport#setUp()
     */
    @Override
    protected void setUp() throws Exception {
        // Create the broker service from the configuration and wait until it
        // has been started...
        broker = new BrokerService();
        SpringSslContext sslContext = new SpringSslContext();
        sslContext.setKeyStorePassword(PASSWORD);
        sslContext.setKeyStore(SERVER_KEYSTORE);
        sslContext.setTrustStore(TRUST_KEYSTORE);
        sslContext.setTrustStorePassword(PASSWORD);
        sslContext.afterPropertiesSet(); // This is required so that the SSLContext instance is generated with the passed information.
        broker.setSslContext(sslContext);
        broker.addConnector(URI_LOCATION);
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.start();
        broker.waitUntilStarted();
        super.setUp();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.AutoFailTestSupport#tearDown()
     */
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        if (broker != null) {
            broker.stop();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.TestSupport#createConnectionFactory()
     */
    @Override
    protected ActiveMQConnectionFactory createConnectionFactory()
        throws Exception {
        ActiveMQSslConnectionFactory factory = new ActiveMQSslConnectionFactory(URI_LOCATION + "?transport.verifyHostName=false");

        // Configure TLS for the client
        factory.setTrustStore(TRUST_KEYSTORE);
        factory.setTrustStorePassword(PASSWORD);

        return factory;
    }
    


}
