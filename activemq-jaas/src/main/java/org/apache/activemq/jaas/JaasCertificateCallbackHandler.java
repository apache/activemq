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

import java.io.IOException;
import java.security.cert.X509Certificate;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

/**
 * A Standard JAAS callback handler for SSL certificate requests. Will only
 * handle callbacks of type CertificateCallback.
 * 
 * @author sepandm@gmail.com (Sepand)
 */
public class JaasCertificateCallbackHandler implements CallbackHandler {
    final X509Certificate certificates[];

    /**
     * Basic constructor.
     * 
     * @param cert The certificate returned when calling back.
     */
    public JaasCertificateCallbackHandler(X509Certificate certs[]) {
        certificates = certs;
    }

    /**
     * Overriding handle method to handle certificates.
     * 
     * @param callbacks The callbacks requested.
     * @throws IOException
     * @throws UnsupportedCallbackException Thrown if an unkown Callback type is
     *                 encountered.
     */
    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (int i = 0; i < callbacks.length; i++) {
            Callback callback = callbacks[i];
            if (callback instanceof CertificateCallback) {
                CertificateCallback certCallback = (CertificateCallback)callback;

                certCallback.setCertificates(certificates);

            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }
}
