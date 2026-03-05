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

import jakarta.jms.Connection;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

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
 * <p>The bug is a lock ordering inversion between two synchronized blocks:
 * <ul>
 *   <li>{@code synchronized(pendingAdditions)} in {@code JDBCMessageStore} — acquired in
 *       {@code addMessage} while calling {@code indexListener.onAdd()}, and later needed by
 *       {@code mc.onCompletion.run()} (cursor-add-complete callback)</li>
 *   <li>{@code synchronized(indexOrderedCursorUpdates)} in {@code Queue} — acquired in
 *       {@code rollbackPendingCursorAdditions} while calling {@code mc.onCompletion.run()}</li>
 * </ul>
 *
 * <p>Deadlock scenario:
 * <ul>
 *   <li>Thread A (rollback path after DB failure): holds {@code indexOrderedCursorUpdates},
 *       calls {@code mc.onCompletion.run()} which needs {@code pendingAdditions}</li>
 *   <li>Thread B (concurrent addMessage): holds {@code pendingAdditions},
 *       calls {@code indexListener.onAdd()} which needs {@code indexOrderedCursorUpdates}</li>
 * </ul>
 *
 * <p>Fix: move {@code mc.onCompletion.run()} outside {@code synchronized(indexOrderedCursorUpdates)}
 * in {@code Queue.rollbackPendingCursorAdditions}.
 *
 * @see <a href="https://github.com/apache/activemq/issues/1731">GitHub #1731</a>
 */
public class H2JDBCDeadlockOnSendExceptionTest {

    private static final String QUEUE_NAME = "test.deadlock.queue";
    /** Property that marks the message whose DB write should fail. */
    private static final String PROP_FAIL = "TEST_FAIL";
    /** Property that marks the concurrent message that helps trigger the deadlock. */
    private static final String PROP_SECOND = "TEST_SECOND";

    private BrokerService broker;
    private ActiveMQConnectionFactory connectionFactory;
    private final CoordinatingIndexListener coordinatingListener = new CoordinatingIndexListener();

    /** Fail the whole test if it takes longer than 30 seconds (indicates a deadlock). */
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
        // Override createQueueMessageStore to intercept registerIndexListener so we can
        // wrap the Queue (IndexListener) with our CoordinatingIndexListener.
        final JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter() {
            @Override
            public MessageStore createQueueMessageStore(final ActiveMQQueue destination) throws IOException {
                final MessageStore base = super.createQueueMessageStore(destination);
                return new ProxyMessageStore(base) {
                    @Override
                    public void registerIndexListener(final IndexListener indexListener) {
                        coordinatingListener.setDelegate(indexListener);
                        super.registerIndexListener(coordinatingListener);
                    }
                };
            }
        };
        jdbc.setDataSource(H2DB.createDataSource("H2JDBCDeadlockTest"));
        jdbc.setAdapter(new FailingH2JDBCAdapter(coordinatingListener));
        jdbc.deleteAllMessages();
        jdbc.setUseLock(false);
        return jdbc;
    }

    /**
     * Verifies that no deadlock occurs when a JDBC exception fires during {@code addMessage}
     * while another {@code addMessage} is executing concurrently.
     *
     * <p>Before the fix: this test hangs and the {@code @Rule Timeout} causes it to fail.
     * After the fix: both threads complete and the test passes.
     */
    @Test
    public void testNoDeadlockOnJDBCException() throws Exception {
        final CountDownLatch rollbackHoldsIndexLock = new CountDownLatch(1);
        final CountDownLatch threadBHoldsPendingLock = new CountDownLatch(1);
        coordinatingListener.setLatches(rollbackHoldsIndexLock, threadBHoldsPendingLock);

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        final CountDownLatch allDone = new CountDownLatch(2);

        // Thread A: sends the message that will fail at the DB layer.
        // JDBCMessageStore.addMessage will call indexListener.onAdd() (adding to
        // indexOrderedCursorUpdates), then doAddMessage throws → rollback path acquires
        // indexOrderedCursorUpdates and calls mc.onCompletion.run().
        executor.execute(() -> {
            try (final Connection conn = connectionFactory.createConnection();
                 final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                conn.start();
                final MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
                final TextMessage msg = session.createTextMessage("will-fail");
                msg.setBooleanProperty(PROP_FAIL, true);
                producer.send(msg);
            } catch (final Exception ignored) {
                // Expected: the broker propagates the IOException from doAddMessage as JMSException.
            } finally {
                allDone.countDown();
            }
        });

        // Wait until Thread A's rollback has acquired indexOrderedCursorUpdates.
        // At this point Thread A is inside mc.onCompletion.run() which is called from
        // rollbackPendingCursorAdditions while holding synchronized(indexOrderedCursorUpdates).
        assertTrue("Thread A rollback should start within 10s",
            rollbackHoldsIndexLock.await(10, TimeUnit.SECONDS));

        // Thread B: sends a normal message concurrently.
        // In JDBCMessageStore.addMessage, Thread B will enter synchronized(pendingAdditions),
        // then call indexListener.onAdd() which tries to acquire indexOrderedCursorUpdates
        // (held by Thread A) → BLOCKED (before fix).
        // Meanwhile, Thread A tries to acquire pendingAdditions (held by Thread B) → DEADLOCK.
        executor.execute(() -> {
            try (final Connection conn = connectionFactory.createConnection();
                 final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                conn.start();
                final MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
                final TextMessage msg = session.createTextMessage("should-succeed");
                msg.setBooleanProperty(PROP_SECOND, true);
                producer.send(msg);
            } catch (final Exception e) {
                // Not expected for Thread B.
            } finally {
                allDone.countDown();
            }
        });

        assertTrue("Both threads should complete without deadlock",
            allDone.await(15, TimeUnit.SECONDS));
        executor.shutdown();
    }

    // -------------------------------------------------------------------------
    // Inner classes
    // -------------------------------------------------------------------------

    /**
     * Wraps the Queue's {@link IndexListener#onAdd} to inject precise timing:
     *
     * <ul>
     *   <li>For the <em>fail</em> message: sets a thread-local flag so the adapter can fail
     *       the DB write, and wraps {@code mc.onCompletion} so that it signals Thread B and
     *       waits for Thread B to hold {@code pendingAdditions} before trying to acquire it.</li>
     *   <li>For the <em>second</em> message: we are called while Thread B holds
     *       {@code pendingAdditions}; we signal Thread A before delegating, which will then
     *       try to acquire {@code indexOrderedCursorUpdates} → deadlock before fix.</li>
     * </ul>
     */
    static class CoordinatingIndexListener implements IndexListener {

        private volatile IndexListener delegate;
        private volatile CountDownLatch rollbackHoldsIndexLock;
        private volatile CountDownLatch threadBHoldsPendingLock;

        /**
         * Thread-local flag: when {@code true}, the adapter must throw on the next
         * {@code doAddMessage} call on this thread.
         */
        final ThreadLocal<Boolean> shouldFailNext = ThreadLocal.withInitial(() -> false);

        void setDelegate(final IndexListener delegate) {
            this.delegate = delegate;
        }

        void setLatches(final CountDownLatch rollbackHoldsIndexLock,
                        final CountDownLatch threadBHoldsPendingLock) {
            this.rollbackHoldsIndexLock = rollbackHoldsIndexLock;
            this.threadBHoldsPendingLock = threadBHoldsPendingLock;
        }

        @Override
        public void onAdd(final IndexListener.MessageContext mc) {
            try {
                if (Boolean.TRUE.equals(mc.message.getProperty(PROP_FAIL))) {
                    // Thread A's fail message: mark thread so adapter fails, and wrap onCompletion.
                    shouldFailNext.set(true);
                    final Runnable originalCompletion = mc.onCompletion;
                    final IndexListener.MessageContext wrapped = new IndexListener.MessageContext(
                        mc.context, mc.message, () -> {
                            // Called from Queue.rollbackPendingCursorAdditions while holding
                            // synchronized(indexOrderedCursorUpdates).
                            rollbackHoldsIndexLock.countDown();
                            // Wait for Thread B to be inside synchronized(pendingAdditions).
                            try {
                                threadBHoldsPendingLock.await(10, TimeUnit.SECONDS);
                            } catch (final InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            // Tries synchronized(pendingAdditions) → DEADLOCK before fix
                            // (Thread B holds pendingAdditions and needs indexOrderedCursorUpdates).
                            originalCompletion.run();
                        });
                    delegate.onAdd(wrapped);

                } else if (Boolean.TRUE.equals(mc.message.getProperty(PROP_SECOND))) {
                    // Thread B's message: we are called from inside synchronized(pendingAdditions)
                    // in JDBCMessageStore.addMessage. Signal Thread A that pendingAdditions is held.
                    threadBHoldsPendingLock.countDown();
                    // Tries synchronized(indexOrderedCursorUpdates) → BLOCKED before fix
                    // (Thread A holds it in rollbackPendingCursorAdditions).
                    delegate.onAdd(mc);

                } else {
                    delegate.onAdd(mc);
                }
            } catch (final IOException e) {
                // Property access failure — fall through to normal processing.
                delegate.onAdd(mc);
            }
        }
    }

    /**
     * H2JDBCAdapter that throws a {@link SQLException} for messages flagged by the
     * {@link CoordinatingIndexListener}'s thread-local.
     *
     * <p>The thread-local is set <em>inside</em> {@code synchronized(pendingAdditions)} (in
     * {@link CoordinatingIndexListener#onAdd}) and read here, which is called on the same thread
     * <em>after</em> the lock is released — so the flag is visible on the same thread.
     */
    static class FailingH2JDBCAdapter extends H2JDBCAdapter {

        private final CoordinatingIndexListener coordinator;

        FailingH2JDBCAdapter(final CoordinatingIndexListener coordinator) {
            this.coordinator = coordinator;
        }

        @Override
        public void doAddMessage(final TransactionContext c, final long sequence,
                                  final MessageId messageID, final ActiveMQDestination destination,
                                  final byte[] data, final long expiration, final byte priority,
                                  final XATransactionId xid) throws SQLException, IOException {
            if (coordinator.shouldFailNext.get()) {
                coordinator.shouldFailNext.set(false);
                throw new SQLException(
                    "Simulated DB failure to reproduce deadlock (GitHub #1731)", "S1000");
            }
            super.doAddMessage(c, sequence, messageID, destination, data, expiration, priority, xid);
        }
    }
}
