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
import org.apache.activemq.JmsTopicSendReceiveTest;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.spring.SpringSslContext;

public class HttpsJmsSendAndReceiveEmbeddedSslConfigTest extends
		JmsTopicSendReceiveTest {

	/**
	 * 
	 */
	private static final String URI_LOCATION = "https://localhost:8161";
	public static final String KEYSTORE_TYPE = "jks";
	public static final String PASSWORD = "password";
	public static final String TRUST_KEYSTORE = "src/test/resources/client.keystore";
    public static final String SERVER_KEYSTORE = "src/test/resources/server.keystore";

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
		System.setProperty("javax.net.ssl.trustStore", TRUST_KEYSTORE);
		System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
		System.setProperty("javax.net.ssl.trustStoreType", KEYSTORE_TYPE);
		// Remove all references of the javax.net.ssl properties that can impact
		// these tests....
		System.getProperties().remove("javax.net.ssl.keyStore");
		System.getProperties().remove("javax.net.ssl.keyStorePassword");
		System.getProperties().remove("javax.net.ssl.keyStoreType");
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
		return new ActiveMQConnectionFactory(URI_LOCATION);
	}

}
