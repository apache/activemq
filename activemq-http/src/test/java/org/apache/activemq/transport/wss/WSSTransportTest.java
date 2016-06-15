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
package org.apache.activemq.transport.wss;

import org.apache.activemq.transport.SecureSocketConnectorFactory;
import org.apache.activemq.transport.ws.WSTransportTest;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

public class WSSTransportTest extends WSTransportTest {
    @Override
    protected Connector createJettyConnector(Server server) throws Exception {
        SecureSocketConnectorFactory sscf = new SecureSocketConnectorFactory();
        sscf.setKeyStore("src/test/resources/server.keystore");
        sscf.setKeyStorePassword("password");
        sscf.setTrustStore("src/test/resources/client.keystore");
        sscf.setTrustStorePassword("password");

        ServerConnector c = (ServerConnector) sscf.createConnector(server);
        c.setPort(getProxyPort());
        return c;
    }

    @Override
    protected String getWSConnectorURI() {
        return "wss://localhost:61623";
    }

    @Override
    protected String getTestURI() {
        int port = getProxyPort();
        return "https://localhost:" + port + "/websocket.html#wss://localhost:61623";
    }
}
