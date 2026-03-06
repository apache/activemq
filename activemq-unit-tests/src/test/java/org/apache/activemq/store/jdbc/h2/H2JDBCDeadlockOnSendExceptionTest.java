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
package org.apache.activemq.store.jdbc.h2;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.jms.Connection;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.store.IndexListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.ProxyMessageStore;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.store.jdbc.TransactionContext;
import org.apache.activemq.store.jdbc.adapter.H2JDBCAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertTrue;

/**
 * Reproduces the JDBC store deadlock described in GitHub issue #1731.
 *
 * <p>Lock-ordering inversion between:
 * <ul>
 *   <li>{@code pendingAdditions} (JDBCMessageStore) — held by {@code addMessage} while calling
 *       {@code indexListener.onAdd()}, and needed later by the cursor-completion callback</li>
 *   <li>{@code indexOrderedCursorUpdates} (Queue) — held by {@code rollbackPendingCursorAdditions}
 *       while calling that same cursor-completion callback</li>
 * </ul>
 *
 * <p>Fix: move {@code mc.onCompletion.run()} outside {@code synchronized(indexOrderedCursorUpdates)}
 * in {@code Queue.rollbackPendingCursorAdditions}.
 *
 * @see <a href="https://github.com/apache/activemq/issues/1731">GitHub #1731</a>
 */
public class H2JDBCDeadlockOnSendExceptionTest {

    private static final String QUEUE_NAME = "test.deadlock.queue";

    private BrokerService broker;
    private ActiveMQConnectionFactory connectionFactory;
    private final DeadlockCoordinator coordinator = new DeadlockCoordinator();

    /** Fail the test if it hangs beyond 30 seconds — that indicates a deadlock. */
    @Rule
    public final Timeout testTimeout = Timeout.seconds(30);

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setPersistenceAdapter(createJDBCAdapter());
        broker.addConnector("tcp://0.0.0.0:0");
        broker.start();
        broker.waitUntilStarted();
        connectionFactory = new ActiveMQConnectionFactory(
            broker.getTransportConnectors().get(0).getPublishableConnectString());
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null && broker.isStarted()) {
            broker.stop();
        }
    }

    private JDBCPersistenceAdapter createJDBCAdapter() throws IOException {
        final JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter() {
            @Override
            public MessageStore createQueueMessageStore(final ActiveMQQueue destination) throws IOException {
                final MessageStore base = super.createQueueMessageStore(destination);
                // Intercept registerIndexListener to wrap Queue with the coordinator.
                return new ProxyMessageStore(base) {
                    @Override
                    public void registerIndexListener(final IndexListener indexListener) {
                        coordinator.setQueueListener(indexListener);
                        super.registerIndexListener(coordinator);
                    }
                };
            }
        };
        jdbc.setDataSource(H2DB.createDataSource("H2JDBCDeadlockTest"));
        jdbc.setAdapter(coordinator.newAdapter());
        jdbc.deleteAllMessages();
        jdbc.setUseLock(false);
        return jdbc;
    }

    /**
     * Verifies that no deadlock occurs when a JDBC exception fires during {@code addMessage}
     * while another {@code addMessage} executes concurrently.
     *
     * <p>Before the fix: test hangs → {@code @Rule Timeout} fails it.
     * After the fix: both threads complete normally.
     */
    @Test
    public void testNoDeadlockOnJDBCException() throws Exception {
        final CountDownLatch rollbackHoldsIndexLock = new CountDownLatch(1);
        final CountDownLatch threadBHoldsPendingLock = new CountDownLatch(1);
        coordinator.arm(rollbackHoldsIndexLock, threadBHoldsPendingLock);

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        final CountDownLatch allDone = new CountDownLatch(2);

        // Thread A: sends the message whose DB write will be injected to fail.
        // The failure triggers Queue.rollbackPendingCursorAdditions, which acquires
        // indexOrderedCursorUpdates and calls mc.onCompletion (inside that lock before fix).
        executor.execute(() -> {
            try (final Connection conn = connectionFactory.createConnection();
                 final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                conn.start();
                session.createProducer(session.createQueue(QUEUE_NAME))
                    .send(session.createTextMessage("will-fail"));
            } catch (final Exception ignored) {
                // Expected: the broker propagates the injected IOException as JMSException.
            } finally {
                allDone.countDown();
            }
        });

        // Wait until Thread A's rollback has acquired indexOrderedCursorUpdates.
        assertTrue("Thread A rollback should start", rollbackHoldsIndexLock.await(10, TimeUnit.SECONDS));

        // Thread B: sends a normal message while Thread A's rollback holds indexOrderedCursorUpdates.
        // Thread B enters synchronized(pendingAdditions) in addMessage, then calls
        // indexListener.onAdd() which needs indexOrderedCursorUpdates → BLOCKED (before fix).
        // Meanwhile Thread A needs pendingAdditions (held by Thread B) → DEADLOCK.
        executor.execute(() -> {
            try (final Connection conn = connectionFactory.createConnection();
                 final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                conn.start();
                session.createProducer(session.createQueue(QUEUE_NAME))
                    .send(session.createTextMessage("should-succeed"));
            } catch (final Exception ignored) {
            } finally {
                allDone.countDown();
            }
        });

        assertTrue("Both threads should complete without deadlock",
            allDone.await(15, TimeUnit.SECONDS));
        executor.shutdown();
    }

    // -------------------------------------------------------------------------

    /**
     * Coordinates the precise timing needed to expose the ABBA lock cycle.
     *
     * <p>Implements {@link IndexListener} to intercept {@code onAdd()} calls:
     * <ul>
     *   <li>For the <em>first</em> message (Thread A's fail message): wraps {@code mc.onCompletion}
     *       so that when it is called from inside {@code synchronized(indexOrderedCursorUpdates)}
     *       it signals Thread B and waits for Thread B to hold {@code pendingAdditions} before
     *       trying to acquire it — completing the deadlock cycle.</li>
     *   <li>For the <em>second</em> message (Thread B): signals Thread A that
     *       {@code pendingAdditions} is held, then calls the real {@code onAdd} which tries
     *       to acquire {@code indexOrderedCursorUpdates} (held by Thread A) — BLOCKED.</li>
     * </ul>
     *
     * <p>Also provides a {@link H2JDBCAdapter} that fails the first {@code doAddMessage}
     * call, which is what pushes Thread A into the rollback path.
     */
    static class DeadlockCoordinator implements IndexListener {

        private IndexListener queueListener;
        private CountDownLatch rollbackHoldsIndexLock;
        private CountDownLatch threadBHoldsPendingLock;

        /** Set to true (on Thread A) in onAdd so the adapter throws on the same thread. */
        private final AtomicBoolean failNextWrite = new AtomicBoolean(false);
        /** Flips to false after Thread A's onCompletion wrapper is registered. */
        private final AtomicBoolean firstMessage = new AtomicBoolean(true);

        void setQueueListener(final IndexListener queueListener) {
            this.queueListener = queueListener;
        }

        void arm(final CountDownLatch rollbackHoldsIndexLock,
                 final CountDownLatch threadBHoldsPendingLock) {
            this.rollbackHoldsIndexLock = rollbackHoldsIndexLock;
            this.threadBHoldsPendingLock = threadBHoldsPendingLock;
        }

        H2JDBCAdapter newAdapter() {
            return new H2JDBCAdapter() {
                @Override
                public void doAddMessage(final TransactionContext c, final long sequence,
                                          final MessageId messageID,
                                          final ActiveMQDestination destination,
                                          final byte[] data, final long expiration,
                                          final byte priority, final XATransactionId xid)
                        throws SQLException, IOException {
                    if (failNextWrite.compareAndSet(true, false)) {
                        throw new SQLException(
                            "Simulated DB failure to reproduce deadlock (GitHub #1731)", "S1000");
                    }
                    super.doAddMessage(c, sequence, messageID, destination,
                        data, expiration, priority, xid);
                }
            };
        }

        @Override
        public void onAdd(final IndexListener.MessageContext mc) {
            if (firstMessage.compareAndSet(true, false)) {
                // Thread A — inside synchronized(pendingAdditions) in JDBCMessageStore.addMessage.
                // Mark the write to fail, then wrap onCompletion with deadlock-triggering logic.
                failNextWrite.set(true);
                final Runnable original = mc.onCompletion;
                final IndexListener.MessageContext wrapped = new IndexListener.MessageContext(
                    mc.context, mc.message, () -> {
                        // Called from Queue.rollbackPendingCursorAdditions while holding
                        // synchronized(indexOrderedCursorUpdates).
                        rollbackHoldsIndexLock.countDown();           // signal Thread B to start
                        try {
                            threadBHoldsPendingLock.await(10, TimeUnit.SECONDS); // wait for Thread B
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        original.run(); // needs pendingAdditions → DEADLOCK if Thread B holds it
                    });
                queueListener.onAdd(wrapped);
            } else {
                // Thread B — also inside synchronized(pendingAdditions).
                // Signal Thread A that pendingAdditions is now held.
                threadBHoldsPendingLock.countDown();
                // Needs indexOrderedCursorUpdates (held by Thread A's rollback) → BLOCKED.
                queueListener.onAdd(mc);
            }
        }
    }
}
