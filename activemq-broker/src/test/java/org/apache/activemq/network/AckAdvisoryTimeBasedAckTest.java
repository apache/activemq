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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.transport.TransportSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.Wait;
import org.apache.activemq.wireformat.WireFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies the time-based advisory ack flush in
 * {@link DemandForwardingBridgeSupport#ackAdvisory}: pending advisory
 * dispatches below the advisoryAckPercentage threshold must still be
 * acknowledged once advisoryAckInterval elapses, so a quiet bridge is not
 * flagged and aborted as a slow consumer by an abortSlowAckConsumerStrategy
 * that does not ignore network consumers.
 */
public class AckAdvisoryTimeBasedAckTest {

    private NetworkBridgeConfiguration configuration;
    private RecordingTransport remoteTransport;
    private DemandForwardingBridge bridge;
    private BrokerService brokerService;
    private ActiveMQMessage advisory;

    @Before
    public void setUp() throws Exception {
        configuration = new NetworkBridgeConfiguration();
        remoteTransport = new RecordingTransport();

        bridge = new DemandForwardingBridge(configuration, new RecordingTransport(), remoteTransport);
        brokerService = new BrokerService();
        // setBrokerService() dereferences the region broker of a started broker;
        // ackAdvisory only needs the task runner factory, so set the field directly
        bridge.brokerService = brokerService;

        var consumerInfo = new ConsumerInfo();
        consumerInfo.setConsumerId(new ConsumerId(new SessionId(new ConnectionId("advisory-time"), 1), 1));
        consumerInfo.setPrefetchSize(1000); // threshold = 1000 * 75% = 750
        bridge.demandConsumerInfo = consumerInfo;

        advisory = new ActiveMQMessage();
        advisory.setMessageId(new MessageId("ID:advisory-time-1:1:1:1"));
        advisory.setDestination(new ActiveMQQueue("ActiveMQ.Advisory.Consumer.Queue.TEST"));
    }

    @After
    public void tearDown() throws Exception {
        if (bridge != null) {
            shutdownExecutor(bridge, "serialExecutor");
            shutdownExecutor(bridge, "syncExecutor");
        }
        if (brokerService != null) {
            brokerService.getTaskRunnerFactory().shutdown();
        }
    }

    @Test(timeout = 30000)
    public void testFlushAcksPendingAfterInterval() throws Exception {
        // dispatch below the percentage threshold while time-based acks cannot fire
        configuration.setAdvisoryAckInterval(60_000);
        for (var i = 0; i < 5; i++) {
            bridge.ackAdvisory(advisory);
        }
        assertEquals(0, ackCount());

        // let the (shortened) interval elapse, then flush as the timer would
        configuration.setAdvisoryAckInterval(50);
        Thread.sleep(80);
        bridge.flushPendingAdvisoryAcks();

        assertTrue("pending advisories should be acked by the interval flush",
                Wait.waitFor(() -> ackedTotal() == 5, 5_000, 50));
        assertEquals(1, ackCount());
    }

    @Test(timeout = 30000)
    public void testFlushIsNoOpBeforeInterval() throws Exception {
        configuration.setAdvisoryAckInterval(60_000);
        for (var i = 0; i < 5; i++) {
            bridge.ackAdvisory(advisory);
        }

        bridge.flushPendingAdvisoryAcks();

        Thread.sleep(100); // acks are sent async; allow a wrong ack to surface
        assertEquals("no ack may be sent before the interval elapses", 0, ackCount());
    }

    @Test(timeout = 30000)
    public void testFlushDisabledWhenIntervalNotPositive() throws Exception {
        configuration.setAdvisoryAckInterval(0);
        for (var i = 0; i < 5; i++) {
            bridge.ackAdvisory(advisory);
        }

        Thread.sleep(80);
        bridge.flushPendingAdvisoryAcks();

        Thread.sleep(100);
        assertEquals("time-based acks are disabled at interval <= 0", 0, ackCount());
    }

    @Test(timeout = 30000)
    public void testDispatchPathAcksWhenIntervalElapsed() throws Exception {
        // a slow trickle must flush via the dispatch path itself, without the timer
        configuration.setAdvisoryAckInterval(60_000);
        bridge.ackAdvisory(advisory);
        assertEquals(0, ackCount());

        configuration.setAdvisoryAckInterval(50);
        Thread.sleep(80);
        bridge.ackAdvisory(advisory);

        assertTrue("a dispatch after the interval must ack the pending batch",
                Wait.waitFor(() -> ackedTotal() == 2, 5_000, 50));
    }

    @Test(timeout = 30000)
    public void testPercentageThresholdStillAcksImmediately() throws Exception {
        configuration.setAdvisoryAckInterval(60_000);
        bridge.demandConsumerInfo.setPrefetchSize(4); // threshold = 3

        for (var i = 0; i < 4; i++) {
            bridge.ackAdvisory(advisory);
        }

        assertTrue("crossing the percentage threshold must ack without waiting",
                Wait.waitFor(() -> ackedTotal() == 4, 5_000, 50));
        assertEquals(1, ackCount());
    }

    private int ackCount() {
        synchronized (remoteTransport.oneways) {
            return remoteTransport.oneways.size();
        }
    }

    private long ackedTotal() {
        var total = 0L;
        synchronized (remoteTransport.oneways) {
            for (var command : remoteTransport.oneways) {
                total += ((MessageAck) command).getMessageCount();
            }
        }
        return total;
    }

    private static void shutdownExecutor(Object target, String fieldName) throws Exception {
        var f = DemandForwardingBridgeSupport.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        ((ExecutorService) f.get(target)).shutdownNow();
    }

    private static class RecordingTransport extends TransportSupport {
        final List<Object> oneways = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void oneway(Object command) {
            oneways.add(command);
        }

        @Override
        public String getRemoteAddress() {
            return "stub://recording";
        }

        @Override
        public int getReceiveCounter() {
            return 0;
        }

        @Override
        public X509Certificate[] getPeerCertificates() {
            return null;
        }

        @Override
        public void setPeerCertificates(X509Certificate[] certificates) {
        }

        @Override
        public WireFormat getWireFormat() {
            return null;
        }

        @Override
        protected void doStart() throws Exception {
        }

        @Override
        protected void doStop(ServiceStopper stopper) throws Exception {
        }
    }
}
