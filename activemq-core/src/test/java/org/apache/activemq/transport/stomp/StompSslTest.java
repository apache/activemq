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
package org.apache.activemq.transport.stomp;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

/**
 * 
 */
public class StompSslTest extends StompTest {

    protected void setUp() throws Exception {
        bindAddress = "stomp+ssl://localhost:61612";
        confUri = "xbean:org/apache/activemq/transport/stomp/sslstomp-auth-broker.xml";
        System.setProperty("javax.net.ssl.trustStore", "src/test/resources/client.keystore");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStoreType", "jks");
        System.setProperty("javax.net.ssl.keyStore", "src/test/resources/server.keystore");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.keyStoreType", "jks");  
        super.setUp();
    }

    protected Socket createSocket(URI connectUri) throws IOException {
        SocketFactory factory = SSLSocketFactory.getDefault();
        return factory.createSocket("127.0.0.1", connectUri.getPort());
    }
}
