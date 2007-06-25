/**
 *
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

package org.apache.activemq.transport.tcp;

import java.io.IOException;
import java.security.cert.X509Certificate;

import javax.management.remote.JMXPrincipal;
import javax.net.ssl.SSLSocket;

import junit.framework.TestCase;

import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.transport.StubTransportListener;
import org.apache.activemq.wireformat.ObjectStreamWireFormat;

/**
 * Unit tests for the SslTransport class.
 * 
 */
public class SslTransportTest extends TestCase {
    
    SSLSocket sslSocket;
    StubTransportListener stubListener;
    
    String username;
    String password;
    String certDistinguishedName;
    
    protected void setUp() throws Exception {
        certDistinguishedName = "ThisNameIsDistinguished";
        username = "SomeUserName";
        password = "SomePassword";
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }
    
    private void createTransportAndConsume( boolean wantAuth, boolean needAuth ) throws IOException {
        JMXPrincipal principal = new JMXPrincipal( certDistinguishedName );
        X509Certificate cert = new StubX509Certificate( principal );
        StubSSLSession sslSession = 
            new StubSSLSession( cert );
        
        sslSocket = new StubSSLSocket( sslSession );
        sslSocket.setWantClientAuth(wantAuth);
        sslSocket.setNeedClientAuth(needAuth);
        
		SslTransport transport = new SslTransport(
        		new ObjectStreamWireFormat(), sslSocket );
        
        stubListener = new StubTransportListener();
        
        transport.setTransportListener( stubListener );
        
        ConnectionInfo sentInfo = new ConnectionInfo();
        
        sentInfo.setUserName(username);
        sentInfo.setPassword(password);
        
        transport.doConsume(sentInfo);
    }
    
    public void testKeepClientUserName() throws IOException {
        createTransportAndConsume(true, true);
        
        final ConnectionInfo receivedInfo =
            (ConnectionInfo) stubListener.getCommands().remove();
        
        X509Certificate receivedCert;
        
        try {
            receivedCert = ((X509Certificate[])receivedInfo.getTransportContext())[0]; 
        } catch (Exception e) {
            receivedCert = null;
        }
        
        if ( receivedCert == null ) {
            fail("Transmitted certificate chain was not attached to ConnectionInfo.");
        }
        
        assertEquals("Received certificate distinguished name did not match the one transmitted.",
                certDistinguishedName, receivedCert.getSubjectDN().getName());
        
    }
}


