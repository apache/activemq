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

import javax.security.auth.callback.Callback;

/**
 * A Callback for SSL certificates.
 * 
 * Will return a certificate chain to its client.
 * 
 * @author sepandm@gmail.com (Sepand)
 *
 */
public class CertificateCallback implements Callback {
    X509Certificate certificates[];
    
    /**
     * Setter for certificate chain.
     * 
     * @param certs The certificates to be returned.
     */
    public void setCertificates(X509Certificate certs[]) {
        certificates = certs;
    }
    
    /**
     * Getter for certificate chain.
     * 
     * @return The certificates being carried.
     */
    public X509Certificate[] getCertificates() {
        return certificates;
    }
}
