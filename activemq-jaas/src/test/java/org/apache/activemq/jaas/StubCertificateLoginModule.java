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

package org.apache.activemq.jaas;

import java.security.cert.X509Certificate;
import java.util.Set;

import javax.security.auth.login.LoginException;

public class StubCertificateLoginModule extends CertificateLoginModule {
    final String userName;
    final Set groupNames;
    
    String lastUserName = null;
    X509Certificate[] lastCertChain = null;
    
    public StubCertificateLoginModule(String userName, Set groupNames) {
        this.userName = userName;
        this.groupNames = groupNames;
    }

    protected String getUserNameForCertificates(X509Certificate[] certs)
            throws LoginException {
        lastCertChain = certs;
        return userName;
    }
    
    protected Set getUserGroups(String username) throws LoginException {
        lastUserName = username;
        return this.groupNames;
    }
}
