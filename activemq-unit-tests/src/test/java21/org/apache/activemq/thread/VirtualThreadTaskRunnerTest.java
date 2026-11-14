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
package org.apache.activemq.thread;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.ref.WeakReference;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Behavior tests for the virtual thread TaskRunner: one thread per runner,
 * wakeup delivery under contention, shutdown bounds and contract, failure
 * logging and thread release. Runners come from TaskRunnerFactory, so the
 * multi-release class loading is covered, and the parameter runs every test
 * through both ways of enabling virtual threads: the factory flag and the
 * system property.
 */
@Category(ParallelTest.class)
@RunWith(Parameterized.class)
public class VirtualThreadTaskRunnerTest {

    /** how the factory is told to use virtual threads */
    public enum Setup {
        FACTORY_FLAG, SYSTEM_PROPERTY
    }

    private static final String RUNNER_LOGGER = "org.apache.activemq.thread.VirtualThreadTaskRunner";
    private static final Thread.UncaughtExceptionHandler SWALLOW = (thread, error) -> { };

    @Parameterized.Parameters(name = "{0}")
    public static Object[] setups() {
        return Setup.values();
    }

    @Parameterized.Parameter
    public Setup setup;

    private TaskRunnerFactory factory;
    private String previousProperty;

    @Before
    public void setUp() {
        previousProperty = System.getProperty(TaskRunnerFactory.USE_VIRTUAL_THREAD_TASK_RUNNER_PROPERTY);
        if (setup == Setup.SYSTEM_PROPERTY) {
            System.setProperty(TaskRunnerFactory.USE_VIRTUAL_THREAD_TASK_RUNNER_PROPERTY, "true");
            factory = new TaskRunnerFactory("VTTest", Thread.NORM_PRIORITY, true, 1000, false, false);
        } else {
            System.clearProperty(TaskRunnerFactory.USE_VIRTUAL_THREAD_TASK_RUNNER_PROPERTY);
            factory = new TaskRunnerFactory("VTTest", Thread.NORM_PRIORITY, true, 1000, false, true);
        }
    }

    @After
    public void tearDown() {
        factory.shutdownNow();
        if (previousProperty == null) {
            System.clearProperty(TaskRunnerFactory.USE_VIRTUAL_THREAD_TASK_RUNNER_PROPERTY);
        } else {
            System.setProperty(TaskRunnerFactory.USE_VIRTUAL_THREAD_TASK_RUNNER_PROPERTY, previousProperty);
        }
    }

    @Test(timeout = 30000)
    public void testFactoryCreatesVirtualThreadRunner() throws Exception {
        var virtual = new AtomicBoolean();
        var ran = new CountDownLatch(1);
        var runner = factory.createTaskRunner(() -> {
            virtual.set(isVirtual(Thread.currentThread()));
            ran.countDown();
            return false;
        }, "factory-check");
        try {
            assertEquals("VirtualThreadTaskRunner", runner.getClass().getSimpleName());
            assertTrue(ran.await(5, TimeUnit.SECONDS));
            assertTrue("task should run on a virtual thread", virtual.get());
        } finally {
            runner.shutdown(1000);
        }
    }

    @Test(timeout = 30000)
    public void testRunsOnSingleLongLivedThread() throws Exception {
        var threads = ConcurrentHashMap.<Thread>newKeySet();
        var completed = new AtomicInteger();
        // returning false after every call parks the runner, so every
        // wakeup is an idle to busy transition
        var runner = factory.createTaskRunner(() -> {
            threads.add(Thread.currentThread());
            completed.incrementAndGet();
            return false;
        }, "single-thread");
        try {
            for (var i = 0; i < 50; i++) {
                var target = completed.get() + 1;
                runner.wakeup();
                within(5000, () -> completed.get() >= target, "iteration " + target);
            }
            assertEquals("all iterations should run on one thread", 1, threads.size());
            assertTrue(isVirtual(threads.iterator().next()));
        } finally {
            runner.shutdown(1000);
        }
    }

    @Test(timeout = 30000)
    public void testWakeupDuringStartup() throws Exception {
        var ran = new CountDownLatch(1);
        var runner = factory.createTaskRunner(() -> {
            ran.countDown();
            return false;
        }, "startup");
        try {
            runner.wakeup();
            assertTrue(ran.await(5, TimeUnit.SECONDS));
        } finally {
            runner.shutdown(1000);
        }
    }

    @Test(timeout = 60000)
    public void testNoLostWakeupUnderConcurrentWakeups() throws Exception {
        final var producerCount = 4;
        final var wakeupsPerProducer = 5000;
        var produced = new AtomicLong();
        var consumed = new AtomicLong();
        var runner = factory.createTaskRunner(() -> {
            if (consumed.get() < produced.get()) {
                consumed.incrementAndGet();
                return true;
            }
            return false;
        }, "storm");
        try {
            var producers = new Thread[producerCount];
            for (var p = 0; p < producerCount; p++) {
                producers[p] = new Thread(() -> {
                    try {
                        for (var i = 0; i < wakeupsPerProducer; i++) {
                            produced.incrementAndGet();
                            runner.wakeup();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }, "storm-producer");
                producers[p].start();
            }
            for (var producer : producers) {
                producer.join(20000);
            }
            var expected = (long) producerCount * wakeupsPerProducer;
            assertEquals(expected, produced.get());
            within(20000, () -> consumed.get() == expected,
                    "consumed " + consumed.get() + " of " + expected);
        } finally {
            runner.shutdown(1000);
        }
    }

    @Test(timeout = 30000)
    public void testSelfShutdownDoesNotDeadlock() throws Exception {
        var runnerRef = new AtomicReference<TaskRunner>();
        var armed = new AtomicBoolean();
        var selfShutdownReturned = new CountDownLatch(1);
        var runner = factory.createTaskRunner(() -> {
            if (!armed.get()) {
                return false;
            }
            armed.set(false);
            try {
                runnerRef.get().shutdown(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            selfShutdownReturned.countDown();
            return false;
        }, "self-shutdown");
        runnerRef.set(runner);
        armed.set(true);
        runner.wakeup();
        assertTrue("shutdown called from the task thread must return",
                selfShutdownReturned.await(10, TimeUnit.SECONDS));
        var elapsed = timedShutdown(runner, 5000);
        assertTrue("external shutdown after self shutdown took " + elapsed + "ms", elapsed < 2500);
    }

    @Test(timeout = 30000)
    public void testShutdownBoundedWhileBusy() throws Exception {
        var iterations = new AtomicLong();
        var runner = factory.createTaskRunner(() -> {
            iterations.incrementAndGet();
            return true;
        }, "busy");
        within(5000, () -> iterations.get() > 0, "task to start");
        var elapsed = timedShutdown(runner, 10000);
        assertTrue("shutdown of a busy runner took " + elapsed + "ms", elapsed < 5000);
        assertStopped(iterations);
    }

    @Test(timeout = 30000)
    public void testShutdownNoArgWaitsForRunningIteration() throws Exception {
        var iterating = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var shutdownReturned = new CountDownLatch(1);
        var runner = factory.createTaskRunner(() -> {
            iterating.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return false;
        }, "blocking-noarg");
        assertTrue(iterating.await(5, TimeUnit.SECONDS));
        var stopper = new Thread(() -> {
            try {
                runner.shutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                shutdownReturned.countDown();
            }
        }, "noarg-stopper");
        stopper.start();
        // same contract as the pooled and dedicated runners: zero timeout
        // waits for the running iteration
        assertFalse("no-arg shutdown returned while iterate() was running",
                shutdownReturned.await(500, TimeUnit.MILLISECONDS));
        release.countDown();
        assertTrue("no-arg shutdown did not return after iterate() completed",
                shutdownReturned.await(10, TimeUnit.SECONDS));
    }

    @Test(timeout = 30000)
    public void testNeverWokenRunnerShutsDownPromptly() throws Exception {
        var runner = factory.createTaskRunner(() -> false, "never-woken");
        // give the parked thread time to start and park
        Thread.sleep(100);
        var elapsed = timedShutdown(runner, 5000);
        assertTrue("shutdown of a parked runner took " + elapsed + "ms", elapsed < 2500);
    }

    @Test(timeout = 30000)
    public void testTaskFailureLogsErrorAndReleasesShutdown() throws Exception {
        var appender = CapturingAppender.attach();
        try {
            var thrown = new CountDownLatch(1);
            var runner = factory.createTaskRunner(() -> {
                Thread.currentThread().setUncaughtExceptionHandler(SWALLOW);
                thrown.countDown();
                throw new RuntimeException("simulated task failure");
            }, "failing");
            assertTrue(thrown.await(5, TimeUnit.SECONDS));
            within(5000, () -> appender.contains("Virtual Thread task runner terminating on unexpected error"),
                    "termination error log");
            var elapsed = timedShutdown(runner, 5000);
            assertTrue("shutdown after task failure took " + elapsed + "ms", elapsed < 2500);
            // a wakeup after the runner died must be a silent no-op
            runner.wakeup();
        } finally {
            appender.detach();
        }
    }

    @Test(timeout = 30000)
    public void testInterruptTerminatesQuietly() throws Exception {
        var appender = CapturingAppender.attach();
        try {
            var taskThread = new AtomicReference<Thread>();
            var runner = factory.createTaskRunner(() -> {
                taskThread.set(Thread.currentThread());
                return false;
            }, "interrupted");
            within(5000, () -> taskThread.get() != null, "task to start");
            taskThread.get().interrupt();
            var elapsed = timedShutdown(runner, 5000);
            assertTrue("shutdown after interrupt took " + elapsed + "ms", elapsed < 2500);
            assertTrue("interrupt must not log an error: " + appender.messages,
                    appender.messages.isEmpty());
        } finally {
            appender.detach();
        }
    }

    @Test(timeout = 30000)
    public void testThreadReleasedAfterShutdown() throws Exception {
        var taskThread = new AtomicReference<Thread>();
        var runner = factory.createTaskRunner(() -> {
            taskThread.set(Thread.currentThread());
            return false;
        }, "release");
        within(5000, () -> taskThread.get() != null, "task to start");
        runner.shutdown(5000);
        var weak = new WeakReference<>(taskThread.getAndSet(null));
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (weak.get() != null) {
            if (System.nanoTime() > deadline) {
                fail("runner thread was not released after shutdown");
            }
            System.gc();
            Thread.sleep(100);
        }
    }

    private static long timedShutdown(TaskRunner runner, long timeout) throws InterruptedException {
        var start = System.nanoTime();
        runner.shutdown(timeout);
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    }

    private static void assertStopped(AtomicLong iterations) throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (true) {
            var snapshot = iterations.get();
            Thread.sleep(200);
            if (iterations.get() == snapshot) {
                return;
            }
            if (System.nanoTime() > deadline) {
                fail("task kept iterating after shutdown");
            }
        }
    }

    private static void within(long millis, BooleanSupplier condition, String description)
            throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("timed out waiting for " + description);
            }
            Thread.sleep(10);
        }
    }

    /**
     * Thread.isVirtual() is a JDK 21 API and this source root compiles at the
     * module release, so it is read reflectively. The build only adds this
     * source root on JDK 21 and later.
     */
    private static boolean isVirtual(Thread thread) {
        try {
            return (Boolean) Thread.class.getMethod("isVirtual").invoke(thread);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Thread.isVirtual() unavailable; JDK 21+ required", e);
        }
    }

    private static final class CapturingAppender extends AbstractAppender {

        final List<String> messages = new CopyOnWriteArrayList<>();

        private CapturingAppender() {
            super("vt-runner-capture", null, null, true, Property.EMPTY_ARRAY);
        }

        static CapturingAppender attach() {
            var appender = new CapturingAppender();
            appender.start();
            ((Logger) LogManager.getLogger(RUNNER_LOGGER)).addAppender(appender);
            return appender;
        }

        void detach() {
            ((Logger) LogManager.getLogger(RUNNER_LOGGER)).removeAppender(this);
            stop();
        }

        boolean contains(String fragment) {
            return messages.stream().anyMatch(message -> message.contains(fragment));
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }
    }
}
