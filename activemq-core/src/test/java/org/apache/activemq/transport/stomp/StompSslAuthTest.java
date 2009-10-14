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
 * @version $Revision: 1461 $
 */
public class StompSslAuthTest extends StompTest {

    
    protected void setUp() throws Exception {
    	
    	// Test mutual authentication on both stomp and standard ssl transports    	   
    	bindAddress = "stomp+ssl://localhost:61612";
        confUri = "xbean:org/apache/activemq/transport/stomp/sslstomp-mutual-auth-broker.xml";
        jmsUri="ssl://localhost:61617";
        
        System.setProperty("javax.net.ssl.trustStore", "src/test/resources/client.keystore");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStoreType", "jks");
        System.setProperty("javax.net.ssl.keyStore", "src/test/resources/server.keystore");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.keyStoreType", "jks");  
        //System.setProperty("javax.net.debug","ssl,handshake");
        super.setUp();
    }

    protected Socket createSocket(URI connectUri) throws IOException {
        SocketFactory factory = SSLSocketFactory.getDefault();
        return factory.createSocket("127.0.0.1", connectUri.getPort());
    }
   
    // NOOP - These operations handled by jaas cert login module
    public void testConnectNotAuthenticatedWrongUser() throws Exception {
    }
    
    public void testConnectNotAuthenticatedWrongPassword() throws Exception {
    }
    
    public void testSendNotAuthorized() throws Exception {
    }
    
    public void testSubscribeNotAuthorized() throws Exception {
    }
    
}
