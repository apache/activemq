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

package org.apache.activemq.transport.tcp;

import java.io.IOException;

import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

public class StubSSLSocket extends SSLSocket {
    
    public static final int UNTOUCHED = -1;
    public static final int FALSE = 0;
    public static final int TRUE = 1;
    
    private int wantClientAuthStatus = UNTOUCHED;
    private int needClientAuthStatus = UNTOUCHED;
    private int useClientModeStatus = UNTOUCHED;
    
    final StubSSLSession session; 
        
    public StubSSLSocket(StubSSLSession ses) {
        this.session = ses;
    }
    
    public void setWantClientAuth(boolean arg0) {
        this.wantClientAuthStatus = (arg0 ? TRUE : FALSE);
    }
    
    public void setNeedClientAuth(boolean arg0) {
        this.needClientAuthStatus = (arg0 ? TRUE : FALSE);
        if ( session != null ) {
            this.session.setIsVerified(arg0);
        }
    }
    
    public void setUseClientMode(boolean arg0) {
        useClientModeStatus = (arg0 ? TRUE : FALSE);
    }
    
    public boolean getWantClientAuth() {
        return (wantClientAuthStatus == TRUE);
    }

    public boolean getNeedClientAuth() {
        return (needClientAuthStatus == TRUE);
    }
    
    public boolean getUseClientMode() {
        return (useClientModeStatus == TRUE );
    }
    
    public int getWantClientAuthStatus() {
        return wantClientAuthStatus;
    }
    
    public int getNeedClientAuthStatus() {
        return needClientAuthStatus;
    }
    
    public int getUseClientModeStatus() {
        return useClientModeStatus;
    }
    
    public SSLSession getSession() {
        return this.session;
    }
    
    
    // --- Stubbed methods ---

    public String[] getSupportedCipherSuites() {
        return null;
    }

    public String[] getEnabledCipherSuites() {
        return null;
    }

    public void setEnabledCipherSuites(String[] arg0) {
    }

    public String[] getSupportedProtocols() {
        return null;
    }

    public String[] getEnabledProtocols() {
        return null;
    }

    public void setEnabledProtocols(String[] arg0) {
    }

    public void addHandshakeCompletedListener(HandshakeCompletedListener arg0) {
    }

    public void removeHandshakeCompletedListener(HandshakeCompletedListener arg0) {
    }

    public void startHandshake() throws IOException {
    }

    public void setEnableSessionCreation(boolean arg0) {
    }

    public boolean getEnableSessionCreation() {
        return false;
    }

}
