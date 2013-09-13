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

package org.apache.activemq.broker;

import java.io.IOException;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.SecureRandom;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

import org.apache.activemq.transport.TransportFactorySupport;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.tcp.SslTransportFactory;

/**
 * A BrokerService that allows access to the key and trust managers used by SSL
 * connections. There is no reason to use this class unless SSL is being used
 * AND the key and trust managers need to be specified from within code. In
 * fact, if the URI passed to this class does not have an "ssl" scheme, this
 * class will pass all work on to its superclass.
 * 
 * @author sepandm@gmail.com (Sepand)
 */
public class SslBrokerService extends BrokerService {
    /**
     * Adds a new transport connector for the given bind address. If the
     * transport created uses SSL, it will also use the key and trust managers
     * provided. Otherwise, this is the same as calling addConnector.
     * 
     * @param bindAddress The address to bind to.
     * @param km The KeyManager to be used.
     * @param tm The trustmanager to be used.
     * @param random The source of randomness for the generator.
     * @return the newly connected and added transport connector.
     * @throws Exception
     */

    public TransportConnector addSslConnector(String bindAddress, KeyManager[] km, TrustManager[] tm, SecureRandom random) throws Exception {
        return addSslConnector(new URI(bindAddress), km, tm, random);
    }

    /**
     * Adds a new transport connector for the given bind address. If the
     * transport created uses SSL, it will also use the key and trust managers
     * provided. Otherwise, this is the same as calling addConnector.
     * 
     * @param bindAddress The URI to bind to.
     * @param km The KeyManager to be used.
     * @param tm The trustmanager to be used.
     * @param random The source of randomness for the generator.
     * @return the newly created and added transport connector.
     * @throws Exception
     */
    public TransportConnector addSslConnector(URI bindAddress, KeyManager[] km, TrustManager[] tm, SecureRandom random) throws Exception {
        return addConnector(createSslTransportServer(bindAddress, km, tm, random));
    }

    /**
     * Creates a TransportServer that uses the given key and trust managers. The
     * last three parameters will be eventually passed to SSLContext.init.
     * 
     * @param brokerURI The URI to bind to.
     * @param km The KeyManager to be used.
     * @param tm The trustmanager to be used.
     * @param random The source of randomness for the generator.
     * @return A new TransportServer that uses the given managers.
     * @throws IOException If cannot handle URI.
     * @throws KeyManagementException Passed on from SSL.
     */
    protected TransportServer createSslTransportServer(URI brokerURI, KeyManager[] km, TrustManager[] tm, SecureRandom random) throws IOException, KeyManagementException {

        if (brokerURI.getScheme().equals("ssl")) {
            // If given an SSL URI, use an SSL TransportFactory and configure
            // it to use the given key and trust managers.
            SslTransportFactory transportFactory = new SslTransportFactory();
            
            SslContext ctx = new SslContext(km, tm, random);
            SslContext.setCurrentSslContext(ctx);
            try {
                return transportFactory.doBind(brokerURI);
            } finally {
                SslContext.setCurrentSslContext(null);
            }
            
        } else {
            // Else, business as usual.
            return TransportFactorySupport.bind(this, brokerURI);
        }
    }
}
