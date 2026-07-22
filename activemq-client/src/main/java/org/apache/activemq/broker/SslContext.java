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
package org.apache.activemq.broker;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;

import javax.net.ssl.SSLContext;

/**
 * Provides an {@link SSLContext} for SSL/TLS transport connectors and
 * network connectors.
 *
 * <p>Implementations may be static (see {@link ThreadLocalSslContext}) or
 * support runtime certificate reload.
 */
public abstract class SslContext {

    /**
     * Returns a fully initialised {@link SSLContext} ready for use by
     * transport factories.
     */
    public abstract SSLContext getSSLContext() throws NoSuchProviderException, NoSuchAlgorithmException, KeyManagementException;

    /**
     * Reload certificates from the underlying key/trust material.
     * The default implementation is a no-op; reloadable implementations
     * override this to swap in new credentials without restarting the
     * broker.
     */
    public void reload() throws Exception {
    }

    /**
     * @deprecated Use explicit parameter passing instead of ThreadLocal propagation.
     */
    @Deprecated
    public static void setCurrentSslContext(SslContext ctx) {
        ThreadLocalSslContext.setCurrent(ctx);
    }

    /**
     * @deprecated Use explicit parameter passing instead of ThreadLocal propagation.
     */
    @Deprecated
    public static SslContext getCurrentSslContext() {
        return ThreadLocalSslContext.getCurrent();
    }
}
