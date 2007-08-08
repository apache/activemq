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
package org.apache.activemq.transport.stomp;

import org.apache.activemq.transport.tcp.SslSocketHelper;

import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.SocketFactory;
import java.net.Socket;
import java.net.URI;
import java.io.IOException;

/**
 * @version $Revision$
 */
public class StompSslTest extends StompTest {

    protected void setUp() throws Exception {
        bindAddress = "stomp+ssl://localhost:0";
        super.setUp();
    }

    protected Socket createSocket(URI connectUri) throws IOException {
        SocketFactory factory = SSLSocketFactory.getDefault();
        return factory.createSocket("127.0.0.1", connectUri.getPort());
    }
}
