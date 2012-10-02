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

import javax.net.ssl.SSLSocket;

import org.apache.activemq.wireformat.WireFormat;

public class StubSslTransport extends SslTransport {
    public static final int UNTOUCHED = -1;
    public static final int FALSE = 0;
    public static final int TRUE = 1;

    private int wantClientAuthStatus = UNTOUCHED;
    private int needClientAuthStatus = UNTOUCHED;

    public StubSslTransport(WireFormat wireFormat, SSLSocket socket) throws Exception {
        super(wireFormat, socket);
    }

    public void setWantClientAuth(boolean arg0) {
        this.wantClientAuthStatus = arg0 ? TRUE : FALSE;
    }

    public void setNeedClientAuth(boolean arg0) {
        this.needClientAuthStatus = arg0 ? TRUE : FALSE;
    }

    public int getWantClientAuthStatus() {
        return wantClientAuthStatus;
    }

    public int getNeedClientAuthStatus() {
        return needClientAuthStatus;
    }
}
