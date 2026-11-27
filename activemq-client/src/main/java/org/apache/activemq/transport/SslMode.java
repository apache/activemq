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
package org.apache.activemq.transport;

/**
 * Whether a transport server asks connecting clients for a certificate.
 */
public enum SslMode {

    /** No client certificate is requested, or the server is not SSL. */
    NONE,

    /** A client certificate is requested, but the client may connect without one. */
    WANT_CLIENT_AUTH,

    /** A client certificate is required. */
    NEED_CLIENT_AUTH;

    public static SslMode of(boolean needClientAuth, boolean wantClientAuth) {
        if (needClientAuth) {
            return NEED_CLIENT_AUTH;
        }
        return wantClientAuth ? WANT_CLIENT_AUTH : NONE;
    }
}
