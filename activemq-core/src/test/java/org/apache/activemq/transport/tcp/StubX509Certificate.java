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

import java.math.BigInteger;
import java.security.Principal;
import java.security.PublicKey;
import java.util.Date;
import java.util.Set;

import java.security.cert.X509Certificate;

public class StubX509Certificate extends X509Certificate {
    public StubX509Certificate(Principal id) {
        this.id = id;
    }
    
    public Principal getSubjectDN() {
        return this.id;
    }
    
    private final Principal id;
    
    // --- Stubbed Methods ---
    public void checkValidity() {
    // TODO Auto-generated method stub
    
    }
    
    public void checkValidity(Date arg0) {
    // TODO Auto-generated method stub
    
    }
    
    public int getVersion() {
    // TODO Auto-generated method stub
    return 0;
    }
    
    public BigInteger getSerialNumber() {
    // TODO Auto-generated method stub
    return null;
    }
    
    public Principal getIssuerDN() {
    // TODO Auto-generated method stub
    return null;
    }
    
    public Date getNotBefore() {
    // TODO Auto-generated method stub
    return null;
    }
    
    public Date getNotAfter() {
    // TODO Auto-generated method stub
    return null;
    }
    
    public byte[] getTBSCertificate() {
    // TODO Auto-generated method stub
    return null;
    }
    
    public byte[] getSignature() {
    // TODO Auto-generated method stub
    return null;
    }
    
    public String getSigAlgName() {
    // TODO Auto-generated method stub
    return null;
    }
    
    public String getSigAlgOID() {
    // TODO Auto-generated method stub
    return null;
    }
    
    public byte[] getSigAlgParams() {
    // TODO Auto-generated method stub
    return null;
    }
    
    public boolean[] getIssuerUniqueID() {
    // TODO Auto-generated method stub
    return null;
    }
    
    public boolean[] getSubjectUniqueID() {
    // TODO Auto-generated method stub
    return null;
    }
    
    public boolean[] getKeyUsage() {
    // TODO Auto-generated method stub
    return null;
    }
    
    public int getBasicConstraints() {
    // TODO Auto-generated method stub
    return 0;
    }
    
    public byte[] getEncoded() {
    // TODO Auto-generated method stub
    return null;
    }
    
    public void verify(PublicKey arg0) {
    // TODO Auto-generated method stub
    
    }
    
    public void verify(PublicKey arg0, String arg1) {
    // TODO Auto-generated method stub
    
    }
    
    public String toString() {
    // TODO Auto-generated method stub
    return null;
    }
    
    public PublicKey getPublicKey() {
    // TODO Auto-generated method stub
    return null;
    }
    
    public boolean hasUnsupportedCriticalExtension() {
    // TODO Auto-generated method stub
    return false;
    }
    
    public Set getCriticalExtensionOIDs() {
    // TODO Auto-generated method stub
    return null;
    }
    
    public Set getNonCriticalExtensionOIDs() {
    // TODO Auto-generated method stub
    return null;
    }
    
    public byte[] getExtensionValue(String arg0) {
    // TODO Auto-generated method stub
    return null;
    }

}
