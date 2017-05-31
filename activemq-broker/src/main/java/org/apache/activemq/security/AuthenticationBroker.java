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

/**
 * Base for all broker plugins that wish to provide connection authentication services
 */
public interface AuthenticationBroker {

    /**
     * Authenticate the given user using the mechanism provided by this service.
     *
     * @param username
     *        the given user name to authenticate, null indicates an anonymous user.
     * @param password
     *        the given password for the user to authenticate.
     * @param peerCertificates
     *        for an SSL channel the certificates from remote peer.
     *
     * @return a new SecurityContext for the authenticated user.
     *
     * @throws SecurityException if the user cannot be authenticated.
     */
    SecurityContext authenticate(String username, String password, X509Certificate[] peerCertificates) throws SecurityException;

}