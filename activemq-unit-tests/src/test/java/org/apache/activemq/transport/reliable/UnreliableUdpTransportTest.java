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
package org.apache.activemq.transport.reliable;

import java.net.SocketAddress;
import java.net.URI;

import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.CommandJoiner;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.udp.ResponseRedirectInterceptor;
import org.apache.activemq.transport.udp.UdpTransport;
import org.apache.activemq.transport.udp.UdpTransportTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 */
public class UnreliableUdpTransportTest extends UdpTransportTest {
    private static final Logger LOG = LoggerFactory.getLogger(UnreliableUdpTransportTest.class);

    protected DropCommandStrategy dropStrategy = new DropCommandStrategy() {

        @Override
        public boolean shouldDropCommand(int commandId, SocketAddress address, boolean redelivery) {
            if (redelivery) {
                return false;
            }
            return commandId % 3 == 2;
        }
    };

    @Override
    protected Transport createProducer() throws Exception {
        LOG.info("Producer using URI: " + producerURI);

        OpenWireFormat wireFormat = createWireFormat();
        UnreliableUdpTransport transport = new UnreliableUdpTransport(wireFormat, new URI(producerURI));
        transport.setDropCommandStrategy(dropStrategy);

        ReliableTransport reliableTransport = new ReliableTransport(transport, transport);
        Replayer replayer = reliableTransport.getReplayer();
        reliableTransport.setReplayStrategy(createReplayStrategy(replayer));

        return new CommandJoiner(reliableTransport, wireFormat);
    }

    @Override
    protected Transport createConsumer() throws Exception {
        LOG.info("Consumer on port: " + consumerPort);
        OpenWireFormat wireFormat = createWireFormat();
        UdpTransport transport = new UdpTransport(wireFormat, consumerPort);

        ReliableTransport reliableTransport = new ReliableTransport(transport, transport);
        Replayer replayer = reliableTransport.getReplayer();
        reliableTransport.setReplayStrategy(createReplayStrategy(replayer));

        ResponseRedirectInterceptor redirectInterceptor = new ResponseRedirectInterceptor(reliableTransport, transport);
        return new CommandJoiner(redirectInterceptor, wireFormat);
    }

    protected ReplayStrategy createReplayStrategy(Replayer replayer) {
        assertNotNull("Should have a replayer!", replayer);
        return new DefaultReplayStrategy(1);
    }

    @Override
    public void testSendingMediumMessage() throws Exception {
        // Ignoring, see AMQ-4973
    }

    @Override
    public void testSendingLargeMessage() throws Exception {
        // Ignoring, see AMQ-4973
    }
}
