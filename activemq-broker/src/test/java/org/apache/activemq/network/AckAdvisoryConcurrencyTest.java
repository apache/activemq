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

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
import org.junit.Test;

/**
 * Verifies that the advisory dispatch counter in
 * {@link DemandForwardingBridgeSupport#ackAdvisory} neither loses nor
 * double-claims dispatches under concurrent advisory delivery.
 *
 * The counter is incremented from remote-transport and executor threads;
 * an unsynchronized read-modify-write (and an unguarded reset to zero)
 * silently drops counted dispatches, so the periodic advisory ack
 * under-acknowledges and the remote advisory prefetch window leaks until
 * demand-subscription creation silently stops.
 *
 * Invariant asserted: sum of all acked message counts + counter residue
 * == total dispatches observed.
 */
public class AckAdvisoryConcurrencyTest {

    private DemandForwardingBridge bridge;
    private BrokerService brokerService;

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

    @Test(timeout = 60000)
    public void testConcurrentAckAdvisoryDoesNotLoseDispatches() throws Exception {
        final var configuration = new NetworkBridgeConfiguration();
        final var localTransport = new RecordingTransport();
        final var remoteTransport = new RecordingTransport();

        bridge = new DemandForwardingBridge(configuration, localTransport, remoteTransport);
        brokerService = new BrokerService();
        // setBrokerService() dereferences the region broker of a started broker;
        // ackAdvisory only needs the task runner factory, so set the field directly
        bridge.brokerService = brokerService;

        var consumerInfo = new ConsumerInfo();
        consumerInfo.setConsumerId(new ConsumerId(new SessionId(new ConnectionId("advisory-storm"), 1), 1));
        consumerInfo.setPrefetchSize(1000); // threshold = 1000 * 75% = 750
        bridge.demandConsumerInfo = consumerInfo;

        final var advisory = new ActiveMQMessage();
        advisory.setMessageId(new MessageId("ID:advisory-storm-1:1:1:1"));
        advisory.setDestination(new ActiveMQQueue("ActiveMQ.Advisory.Consumer.Queue.TEST"));

        final var threadCount = 8;
        final var perThread = 100_000;
        final var barrier = new CyclicBarrier(threadCount);

        var pool = Executors.newFixedThreadPool(threadCount);
        try {
            var futures = new ArrayList<Future<?>>();
            for (var t = 0; t < threadCount; t++) {
                futures.add(pool.submit(() -> {
                    barrier.await();
                    for (var i = 0; i < perThread; i++) {
                        bridge.ackAdvisory(advisory);
                    }
                    return null;
                }));
            }
            // get() propagates worker failures that a raw thread would swallow
            for (var future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        final var expected = (long) threadCount * perThread;

        // acks are sent async on the task runner; wait for the claimed total to
        // converge (post-fix it reaches exactly `expected`; pre-fix it stalls short)
        Wait.waitFor(() -> ackedTotal(remoteTransport) + counterResidue(bridge) == expected, 10_000, 100);

        assertEquals("acked dispatch counts + counter residue must equal total dispatches"
                        + " (lost updates in the advisory dispatch counter)",
                expected, ackedTotal(remoteTransport) + counterResidue(bridge));
    }

    private static long ackedTotal(RecordingTransport remote) {
        var total = 0L;
        synchronized (remote.oneways) {
            for (var command : remote.oneways) {
                total += ((MessageAck) command).getMessageCount();
            }
        }
        return total;
    }

    /** Reads the counter for both field shapes: plain int and AtomicInteger (a Number). */
    private static int counterResidue(DemandForwardingBridgeSupport bridge) throws Exception {
        var f = DemandForwardingBridgeSupport.class.getDeclaredField("demandConsumerDispatched");
        f.setAccessible(true);
        return ((Number) f.get(bridge)).intValue();
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
