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
package org.apache.activemq.network;

import java.net.URI;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.SocketProxy;

/**
 * Test network reconnects over tunnels. This case can be especially tricky since a tunnel
 * accepts the connection before the far end is reachable, which fools the TCP transport into
 * thinking that it is connected. The tunnels used to be ssh port forwards to localhost, they
 * are now in-JVM TCP proxies that behave the same way without needing ssh access.
 */
public class SSHTunnelNetworkReconnectTest extends NetworkReconnectTest {

    // the equivalent of ssh -L60006:localhost:61616 and ssh -L60007:localhost:61617,
    // the ports the broker configs point their network connectors at
    private SocketProxy tunnelToBroker1;
    private SocketProxy tunnelToBroker2;

    @Override
    protected BrokerService createFirstBroker() throws Exception {
        return BrokerFactory
            .createBroker(new URI("xbean:org/apache/activemq/network/ssh-reconnect-broker1.xml"));
    }

    @Override
    protected BrokerService createSecondBroker() throws Exception {
        return BrokerFactory
            .createBroker(new URI("xbean:org/apache/activemq/network/ssh-reconnect-broker2.xml"));
    }

    @Override
    protected void setUp() throws Exception {
        tunnelToBroker1 = new SocketProxy(60006, new URI("tcp://localhost:61616"));
        tunnelToBroker2 = new SocketProxy(60007, new URI("tcp://localhost:61617"));
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        if (tunnelToBroker1 != null) {
            tunnelToBroker1.close();
        }
        if (tunnelToBroker2 != null) {
            tunnelToBroker2.close();
        }
    }
}
