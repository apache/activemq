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

package org.apache.activemq.security;

import java.security.cert.X509Certificate;
import java.util.Set;

import javax.security.auth.Subject;

/**
 * Extends the SecurityContext to provide a username which is the
 * Distinguished Name from the certificate.
 *
 */
public class JaasCertificateSecurityContext extends SecurityContext {

    private Subject subject;
    private X509Certificate[] certs;
  
    public JaasCertificateSecurityContext(String userName, Subject subject, X509Certificate[] certs) {
        super(userName);
        this.subject = subject;
        this.certs = certs;
    }

    public Set getPrincipals() {
        return subject.getPrincipals();
    }
  
    public String getUserName() {
        if (certs != null && certs.length > 0) {
            return certs[0].getSubjectDN().getName();
        }
        return super.getUserName();
    }

}
