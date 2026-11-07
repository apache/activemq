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
package org.apache.activemq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.Session;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.failover.FailoverTransport;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Client side coverage for {@code useVirtualThreadTaskRunner}: message
 * listeners run on virtual threads when the flag is set via the broker URL,
 * and {@code Connection.stop()} keeps its JMS contract of blocking until an
 * in-progress listener returns.
 */
@Category(ParallelTest.class)
public class VirtualThreadSessionTaskRunnerTest {

    private static final ActiveMQQueue QUEUE = new ActiveMQQueue("TEST.VIRTUAL.THREAD");

    private BrokerService broker;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test(timeout = 30000)
    public void testListenerRunsOnVirtualThreadWhenEnabled() throws Exception {
        assertTrue(dispatchThreadIsVirtual(true));
    }

    @Test(timeout = 30000)
    public void testListenerRunsOnPlatformThreadByDefault() throws Exception {
        assertFalse(dispatchThreadIsVirtual(false));
    }

    /**
     * JMS requires stop() to block until message listeners in progress have
     * completed. The consumer dispatches under its channel mutex and stop()
     * takes the same mutex, so this holds however the session task is run;
     * the test confirms the contract end to end with virtual thread dispatch.
     */
    @Test(timeout = 30000)
    public void testStopWaitsForInProgressListener() throws Exception {
        var listenerEntered = new CountDownLatch(1);
        var releaseListener = new CountDownLatch(1);
        var stopReturned = new CountDownLatch(1);
        var stopError = new AtomicReference<Throwable>();

        try (var connection = factory(true).createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var consumer = session.createConsumer(QUEUE);
             var producer = session.createProducer(QUEUE)) {

            consumer.setMessageListener(message -> {
                listenerEntered.countDown();
                try {
                    releaseListener.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            connection.start();
            producer.send(session.createTextMessage("block"));
            assertTrue("listener never invoked", listenerEntered.await(10, TimeUnit.SECONDS));

            var stopper = new Thread(() -> {
                try {
                    connection.stop();
                } catch (Throwable t) {
                    stopError.set(t);
                } finally {
                    stopReturned.countDown();
                }
            }, "connection-stopper");
            stopper.start();

            assertFalse("stop() returned while the listener was still running",
                    stopReturned.await(500, TimeUnit.MILLISECONDS));

            releaseListener.countDown();
            assertTrue("stop() did not return after the listener completed",
                    stopReturned.await(10, TimeUnit.SECONDS));
            assertEquals(null, stopError.get());
        }
    }

    /**
     * TaskRunner.shutdown() must block until a running iterate() returns, the
     * same as PooledTaskRunner. A Condition.await(0) returns at once where
     * Object.wait(0) waits, so the virtual runner has to special case zero.
     */
    @Test(timeout = 30000)
    public void testVirtualRunnerShutdownWaitsForRunningIteration() throws Exception {
        assertShutdownWaitsForIteration(true);
    }

    @Test(timeout = 30000)
    public void testPooledRunnerShutdownWaitsForRunningIteration() throws Exception {
        assertShutdownWaitsForIteration(false);
    }

    private void assertShutdownWaitsForIteration(boolean useVirtualThreadTaskRunner) throws Exception {
        var factory = new TaskRunnerFactory("ActiveMQ Test Task", Thread.NORM_PRIORITY, true, 1000, false,
                useVirtualThreadTaskRunner);
        var iterating = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var shutdownReturned = new CountDownLatch(1);
        try {
            var runner = factory.createTaskRunner(() -> {
                iterating.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return false;
            }, "blocking-task");
            runner.wakeup();
            assertTrue("iterate() never started", iterating.await(10, TimeUnit.SECONDS));

            var stopper = new Thread(() -> {
                try {
                    runner.shutdown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    shutdownReturned.countDown();
                }
            }, "runner-stopper");
            stopper.start();

            assertFalse("shutdown() returned while iterate() was still running",
                    shutdownReturned.await(500, TimeUnit.MILLISECONDS));

            release.countDown();
            assertTrue("shutdown() did not return after iterate() completed",
                    shutdownReturned.await(10, TimeUnit.SECONDS));
        } finally {
            factory.shutdownNow();
        }
    }

    /**
     * The failover reconnect task is a separate TaskRunnerFactory. The factory
     * level flag has to reach it, and transportResumed() is delivered from the
     * reconnect task's thread, which is how the test observes it.
     */
    @Test(timeout = 60000)
    public void testFailoverReconnectRunsOnVirtualThreadWhenEnabled() throws Exception {
        assertTrue(failoverReconnectThreadIsVirtual(true, null));
    }

    @Test(timeout = 60000)
    public void testFailoverReconnectRunsOnPlatformThreadByDefault() throws Exception {
        assertFalse(failoverReconnectThreadIsVirtual(false, null));
    }

    /** a value on the failover URI is final in both directions; the factory flag only fills a gap */
    @Test(timeout = 60000)
    public void testFailoverUriOptionOverridesFactoryFlag() throws Exception {
        assertFalse("URI false must win over the factory's true", failoverReconnectThreadIsVirtual(true, false));
        assertTrue("URI true must win over the factory's false", failoverReconnectThreadIsVirtual(false, true));
    }

    /**
     * A transport built outside a connection factory has nothing pushing a value, so
     * the system property is the default; a URI value still wins over the property.
     */
    @Test(timeout = 30000)
    public void testUnsetTransportFollowsSystemPropertyDefault() throws Exception {
        var previous = System.getProperty(TaskRunnerFactory.USE_VIRTUAL_THREAD_TASK_RUNNER_PROPERTY);
        System.setProperty(TaskRunnerFactory.USE_VIRTUAL_THREAD_TASK_RUNNER_PROPERTY, "true");
        try {
            var unset = (FailoverTransport) TransportFactory.connect(new URI("failover:(vm://localhost?create=false)")).narrow(FailoverTransport.class);
            assertEquals("nothing recorded", null, unset.getUseVirtualThreadTaskRunner());
            assertTrue("property default applies", unset.isUseVirtualThreadTaskRunner());

            var uriFalse = (FailoverTransport) TransportFactory.connect(new URI("failover:(vm://localhost?create=false)?useVirtualThreadTaskRunner=false")).narrow(FailoverTransport.class);
            assertFalse("URI false wins over the property", uriFalse.isUseVirtualThreadTaskRunner());
        } finally {
            if (previous == null) {
                System.clearProperty(TaskRunnerFactory.USE_VIRTUAL_THREAD_TASK_RUNNER_PROPERTY);
            } else {
                System.setProperty(TaskRunnerFactory.USE_VIRTUAL_THREAD_TASK_RUNNER_PROPERTY, previous);
            }
        }
    }

    private boolean failoverReconnectThreadIsVirtual(boolean factoryFlag, Boolean transportOption) throws Exception {
        var url = "failover:(vm://localhost?create=false)?initialReconnectDelay=10&maxReconnectDelay=100"
                + "&jms.useVirtualThreadTaskRunner=" + factoryFlag
                + (transportOption == null ? "" : "&useVirtualThreadTaskRunner=" + transportOption);
        var expected = transportOption != null ? transportOption : factoryFlag;
        // a factory left at false says nothing, so the transport records only true or the URI value
        var recorded = transportOption != null ? transportOption : (factoryFlag ? Boolean.TRUE : null);
        var resumed = new CountDownLatch(1);
        var virtual = new AtomicBoolean();

        try (var connection = (ActiveMQConnection) new ActiveMQConnectionFactory(url).createConnection()) {
            connection.start();
            var failover = connection.getTransport().narrow(FailoverTransport.class);
            assertEquals(expected, failover.isUseVirtualThreadTaskRunner());
            assertEquals("what was recorded on the transport", recorded, failover.getUseVirtualThreadTaskRunner());

            connection.addTransportListener(new TransportListener() {
                @Override
                public void onCommand(Object command) {
                }

                @Override
                public void onException(IOException error) {
                }

                @Override
                public void transportInterupted() {
                }

                @Override
                public void transportResumed() {
                    virtual.set(isVirtual(Thread.currentThread()));
                    resumed.countDown();
                }
            });

            // Bounce the broker so failover has to reconnect
            restartBroker();
            assertTrue("failover never reconnected", resumed.await(30, TimeUnit.SECONDS));
        }
        return virtual.get();
    }

    private void restartBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.start();
        broker.waitUntilStarted();
    }

    private boolean dispatchThreadIsVirtual(boolean useVirtualThreadTaskRunner) throws Exception {
        var received = new CountDownLatch(1);
        var virtual = new AtomicBoolean();

        try (var connection = factory(useVirtualThreadTaskRunner).createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var consumer = session.createConsumer(QUEUE);
             var producer = session.createProducer(QUEUE)) {

            consumer.setMessageListener(message -> {
                virtual.set(isVirtual(Thread.currentThread()));
                received.countDown();
            });
            connection.start();
            producer.send(session.createTextMessage("probe"));
            assertTrue("message not delivered", received.await(10, TimeUnit.SECONDS));
            assertTrue(Wait.waitFor(() -> received.getCount() == 0, 1000, 10));
        }
        return virtual.get();
    }

    /**
     * Thread.isVirtual() is a JDK 21 API and this module compiles tests for an
     * older release, so reach it reflectively. This source root is only added
     * to the build on JDK 21 and later.
     */
    private static boolean isVirtual(Thread thread) {
        try {
            return (Boolean) Thread.class.getMethod("isVirtual").invoke(thread);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Thread.isVirtual() unavailable; JDK 21+ required", e);
        }
    }

    private static ActiveMQConnectionFactory factory(boolean useVirtualThreadTaskRunner) {
        return new ActiveMQConnectionFactory(
                "vm://localhost?create=false&jms.useVirtualThreadTaskRunner=" + useVirtualThreadTaskRunner);
    }
}
