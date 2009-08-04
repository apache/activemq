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

import java.security.SecureRandom;
import javax.jms.JMSException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

import org.apache.activemq.broker.SslContext;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.tcp.SslTransportFactory;
import org.apache.activemq.util.JMSExceptionSupport;

/**
 * An ActiveMQConnectionFactory that allows access to the key and trust managers
 * used for SslConnections. There is no reason to use this class unless SSL is
 * being used AND the key and trust managers need to be specified from within
 * code. In fact, if the URI passed to this class does not have an "ssl" scheme,
 * this class will pass all work on to its superclass.
 * 
 * @author sepandm@gmail.com
 */
public class ActiveMQSslConnectionFactory extends ActiveMQConnectionFactory {
    // The key and trust managers used to initialize the used SSLContext.
    protected KeyManager[] keyManager;
    protected TrustManager[] trustManager;
    protected SecureRandom secureRandom;

    /**
     * Sets the key and trust managers used when creating SSL connections.
     * 
     * @param km The KeyManagers used.
     * @param tm The TrustManagers used.
     * @param random The SecureRandom number used.
     */
    public void setKeyAndTrustManagers(final KeyManager[] km, final TrustManager[] tm, final SecureRandom random) {
        keyManager = km;
        trustManager = tm;
        secureRandom = random;
    }

    /**
     * Overriding to make special considerations for SSL connections. If we are
     * not using SSL, the superclass's method is called. If we are using SSL, an
     * SslConnectionFactory is used and it is given the needed key and trust
     * managers.
     * 
     * @author sepandm@gmail.com
     */
    protected Transport createTransport() throws JMSException {
        // If the given URI is non-ssl, let superclass handle it.
        if (!brokerURL.getScheme().equals("ssl")) {
            return super.createTransport();
        }

        try {
            SslTransportFactory sslFactory = new SslTransportFactory();
            SslContext ctx = new SslContext(keyManager, trustManager, secureRandom);
            SslContext.setCurrentSslContext(ctx);
            return sslFactory.doConnect(brokerURL);
        } catch (Exception e) {
            throw JMSExceptionSupport.create("Could not create Transport. Reason: " + e, e);
        }
    }
}
