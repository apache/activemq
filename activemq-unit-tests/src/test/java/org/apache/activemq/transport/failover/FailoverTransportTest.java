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
package org.apache.activemq.transport.failover;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;

import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.state.ConnectionStateTracker;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class FailoverTransportTest {

    protected Transport transport;
    protected FailoverTransport failoverTransport;

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        if (transport != null) {
            transport.stop();
        }
    }

    @Test(timeout = 30000)
    @Ignore("Test fails on windows")
    public void testReconnectUnlimited() throws Exception {

        Transport transport = TransportFactory.connect(
                new URI("failover://(tcp://0.0.0.0:61616)?useExponentialBackOff=false&reconnectDelay=0&initialReconnectDelay=0"));

        transport.setTransportListener(new TransportListener() {

            public void onCommand(Object command) {
            }

            public void onException(IOException error) {
            }

            public void transportInterupted() {
            }

            public void transportResumed() {
            }
        });
        transport.start();

        this.failoverTransport = transport.narrow(FailoverTransport.class);

        assertTrue("no implicit limit of 1000", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return failoverTransport.getConnectFailures() > 1002;
            }
        }));
    }

    @Test(timeout=30000)
    public void testCommandsIgnoredWhenOffline() throws Exception {
        this.transport = createTransport();

        assertNotNull(failoverTransport);

        ConnectionStateTracker tracker = failoverTransport.getStateTracker();
        assertNotNull(tracker);

        ConnectionId id = new ConnectionId("1");
        ConnectionInfo connection = new ConnectionInfo(id);

        // Track a connection
        tracker.track(connection);
        try {
            this.transport.oneway(new RemoveInfo(new ConnectionId("1")));
        } catch(Exception e) {
            fail("Should not have failed to remove this known connection");
        }

        try {
            this.transport.oneway(new RemoveInfo(new ConnectionId("2")));
        } catch(Exception e) {
            fail("Should not have failed to remove this unknown connection");
        }

        this.transport.oneway(new MessageAck());
        this.transport.oneway(new ShutdownInfo());
    }

    @Test(timeout=30000)
    public void testResponsesSentWhenRequestForIgnoredCommands() throws Exception {
        this.transport = createTransport();
        assertNotNull(failoverTransport);
        MessageAck ack = new MessageAck();
        assertNotNull("Should have received a Response", this.transport.request(ack));
        RemoveInfo info = new RemoveInfo(new ConnectionId("2"));
        assertNotNull("Should have received a Response", this.transport.request(info));
    }

    @Test
    public void testLocalhostPortSyntax() throws Exception {
        transport = TransportFactory.connect(
                new URI("failover://(tcp://localhost:1111/localhost:2111)"));

        transport.setTransportListener(new TransportListener() {

            public void onCommand(Object command) {
            }

            public void onException(IOException error) {
            }

            public void transportInterupted() {
            }

            public void transportResumed() {
            }
        });

        failoverTransport = transport.narrow(FailoverTransport.class);

        transport.start();

    }

    protected Transport createTransport() throws Exception {
        Transport transport = TransportFactory.connect(
                new URI("failover://(tcp://localhost:1234?transport.connectTimeout=10000)"));
        transport.setTransportListener(new TransportListener() {

            public void onCommand(Object command) {
            }

            public void onException(IOException error) {
            }

            public void transportInterupted() {
            }

            public void transportResumed() {
            }
        });
        transport.start();

        this.failoverTransport = transport.narrow(FailoverTransport.class);

        return transport;
    }

}
