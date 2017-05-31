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

package org.apache.activemq;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;

import org.junit.Test;

public class ActiveMQSslConnectionFactoryTest {

    final String TRUST_STORE_FILE_NAME = "client.keystore";
    final String TRUST_STORE_PKCS12_FILE_NAME = "client-pkcs12.keystore";
    final String TRUST_STORE_DIRECTORY_NAME = "src/test/resources/ssl/";
    final String TRUST_STORE_RESOURCE_PREFIX = "ssl/";
    final String TRUST_STORE_PASSWORD = "password";
    final String SSL_TRANSPORT = "ssl://localhost:0";
    final String FAILOVER_SSL_TRANSPORT = "failover:(" + SSL_TRANSPORT + ")?maxReconnectAttempts=1";

    @Test(expected = ConnectException.class)
    public void validTrustStoreFileTest() throws Throwable {
        executeTest(SSL_TRANSPORT, TRUST_STORE_DIRECTORY_NAME + TRUST_STORE_FILE_NAME);
    }

    @Test(expected = ConnectException.class)
    public void validTrustStoreURLTest() throws Throwable {
        executeTest(SSL_TRANSPORT, new File(TRUST_STORE_DIRECTORY_NAME + TRUST_STORE_FILE_NAME).toURI().toString());
    }

    @Test(expected = ConnectException.class)
    public void validTrustStoreResourceTest() throws Throwable {
        executeTest(SSL_TRANSPORT, TRUST_STORE_RESOURCE_PREFIX + TRUST_STORE_FILE_NAME);
    }

    @Test(expected = IOException.class)
    public void invalidTrustStoreFileTest() throws Throwable {
        executeTest(SSL_TRANSPORT, TRUST_STORE_DIRECTORY_NAME + TRUST_STORE_FILE_NAME + ".dummy");
    }

    @Test(expected = IOException.class)
    public void invalidTrustStoreURLTest() throws Throwable {
        executeTest(SSL_TRANSPORT, new File(TRUST_STORE_DIRECTORY_NAME + TRUST_STORE_FILE_NAME + ".dummy").toURI().toString());
    }

    @Test(expected = IOException.class)
    public void invalidTrustStoreResourceTest() throws Throwable {
        executeTest(SSL_TRANSPORT, TRUST_STORE_RESOURCE_PREFIX + TRUST_STORE_FILE_NAME + ".dummy");
    }

    @Test(expected = IOException.class)
    public void validTrustStoreFileFailoverTest() throws Throwable {
        executeTest(FAILOVER_SSL_TRANSPORT, TRUST_STORE_DIRECTORY_NAME + TRUST_STORE_FILE_NAME);
    }

    @Test(expected = IOException.class)
    public void validTrustStoreURLFailoverTest() throws Throwable {
        executeTest(FAILOVER_SSL_TRANSPORT, new File(TRUST_STORE_DIRECTORY_NAME + TRUST_STORE_FILE_NAME).toURI().toString());
    }

    @Test(expected = IOException.class)
    public void validTrustStoreResourceFailoverTest() throws Throwable {
        executeTest(FAILOVER_SSL_TRANSPORT, TRUST_STORE_RESOURCE_PREFIX + TRUST_STORE_FILE_NAME);
    }

    @Test(expected = IOException.class)
    public void invalidTrustStoreFileFailoverTest() throws Throwable {
        executeTest(FAILOVER_SSL_TRANSPORT, TRUST_STORE_DIRECTORY_NAME + TRUST_STORE_FILE_NAME + ".dummy");
    }

    @Test(expected = IOException.class)
    public void invalidTrustStoreURLFailoverTest() throws Throwable {
        executeTest(FAILOVER_SSL_TRANSPORT, new File(TRUST_STORE_DIRECTORY_NAME + TRUST_STORE_FILE_NAME + ".dummy").toURI().toString());
    }

    @Test(expected = IOException.class)
    public void invalidTrustStoreResourceFailoverTest() throws Throwable {
        executeTest(FAILOVER_SSL_TRANSPORT, TRUST_STORE_RESOURCE_PREFIX + TRUST_STORE_FILE_NAME + ".dummy");
    }

    @Test(expected = ConnectException.class)
    public void validPkcs12TrustStoreFileTest() throws Throwable {
        executeTest(SSL_TRANSPORT, TRUST_STORE_DIRECTORY_NAME + TRUST_STORE_PKCS12_FILE_NAME, "pkcs12");
    }

    @Test(expected = ConnectException.class)
    public void validPkcs12TrustStoreURLTest() throws Throwable {
        executeTest(SSL_TRANSPORT, new File(TRUST_STORE_DIRECTORY_NAME + TRUST_STORE_PKCS12_FILE_NAME).toURI().toString(), "pkcs12");
    }

    @Test(expected = ConnectException.class)
    public void validPkcs12TrustStoreResourceTest() throws Throwable {
        executeTest(SSL_TRANSPORT, TRUST_STORE_RESOURCE_PREFIX + TRUST_STORE_PKCS12_FILE_NAME, "pkcs12");
    }

    @Test(expected = IOException.class)	// Invalid keystore format
    public void invalidTrustStoreTypeTest() throws Throwable {
        executeTest(SSL_TRANSPORT, TRUST_STORE_RESOURCE_PREFIX + TRUST_STORE_PKCS12_FILE_NAME, "jks");
    }

    protected void executeTest(String transport, String name) throws Throwable {
    	executeTest(transport, name, null);
    }

    protected ActiveMQSslConnectionFactory getFactory(String transport) {
        return new ActiveMQSslConnectionFactory(transport);
    }

    protected void executeTest(String transport, String name, String type) throws Throwable {
        try {
            ActiveMQSslConnectionFactory activeMQSslConnectionFactory = getFactory(transport);
            activeMQSslConnectionFactory.setTrustStoreType(type != null ? type : activeMQSslConnectionFactory.getTrustStoreType());
            activeMQSslConnectionFactory.setTrustStore(name);
            activeMQSslConnectionFactory.setTrustStorePassword(TRUST_STORE_PASSWORD);

            javax.jms.Connection connection = activeMQSslConnectionFactory.createConnection();
            connection.start();
            connection.stop();
        } catch (javax.jms.JMSException e) {
            e.getCause().printStackTrace();
            throw e.getCause();
        }
    }
}
