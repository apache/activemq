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
package org.apache.activemq.transport.amqp;

import java.security.SecureRandom;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the JMS client when connected to the SSL transport.
 */
public class JMSClientSslTest extends JMSClientTest {
    protected static final Logger LOG = LoggerFactory.getLogger(JMSClientSslTest.class);

    @BeforeClass
    public static void beforeClass() throws Exception {
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(new KeyManager[0], new TrustManager[]{new DefaultTrustManager()}, new SecureRandom());
        SSLContext.setDefault(ctx);
    }

    @Override
    protected Connection createConnection(String clientId, boolean syncPublish, boolean useSsl) throws JMSException {
        LOG.debug("JMSClientSslTest.createConnection called with clientId {} syncPublish {} useSsl {}", clientId, syncPublish, useSsl);
        return super.createConnection(clientId, syncPublish, true);
    }

    @Override
    protected int getBrokerPort() {
        LOG.debug("JMSClientSslTest.getBrokerPort returning sslPort {}", sslPort);
        return sslPort;
    }

    @Override
    protected boolean isUseTcpConnector() {
        return false;
    }

    @Override
    protected boolean isUseSslConnector() {
        return true;
    }

    @Override
    protected String getTargetConnectorName() {
        return "amqp+ssl";
    }
}
