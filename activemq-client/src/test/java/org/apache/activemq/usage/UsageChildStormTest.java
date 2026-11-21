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
package org.apache.activemq.usage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Microbenchmark for Usage child registration under a create/destroy storm.
 * Many child usages start and then stop against one parent, which is what a
 * broker does when it wires one MemoryUsage per destination into the parent
 * SystemUsage. start() calls parent.addChild and stop() calls
 * parent.removeChild, so this measures those two calls in isolation.
 *
 * Sized small by default so it can run as a regular test. For benchmark runs
 * scale it up, for example:
 *
 *   -Dusage.children=100000 -Dusage.threads=16
 */
public class UsageChildStormTest {

    private MemoryUsage parent;

    @Before
    public void setUp() {
        parent = new MemoryUsage("usage-storm-parent");
        parent.setLimit(Long.MAX_VALUE / 2);
        parent.start();
    }

    @After
    public void tearDown() {
        if (parent != null) {
            parent.stop();
        }
    }

    private interface IndexedOp {
        void apply(int index) throws Exception;
    }

    private long runPhase(String phase, int threadCount, int opCount, IndexedOp op) throws Exception {
        final Queue<Throwable> failures = new ConcurrentLinkedQueue<>();
        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threadCount);
        final Thread[] workers = new Thread[threadCount];

        for (int t = 0; t < threadCount; t++) {
            final int firstIndex = t * opCount / threadCount;
            final int lastIndex = (t + 1) * opCount / threadCount;
            workers[t] = new Thread(() -> {
                try {
                    startGate.await();
                    for (int i = firstIndex; i < lastIndex; i++) {
                        op.apply(i);
                    }
                } catch (Throwable e) {
                    failures.add(e);
                } finally {
                    done.countDown();
                }
            }, "usage-storm-" + phase + "-" + t);
            workers[t].start();
        }

        final long startNanos = System.nanoTime();
        startGate.countDown();
        assertTrue("phase " + phase + " did not finish", done.await(20, TimeUnit.MINUTES));
        final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        for (Thread worker : workers) {
            worker.join(TimeUnit.MINUTES.toMillis(5));
        }
        if (!failures.isEmpty()) {
            throw new AssertionError("phase " + phase + " failed: " + failures.peek(), failures.peek());
        }
        return elapsedMillis;
    }

    @Test(timeout = 1_800_000)
    public void testAddRemoveChildrenStorm() throws Exception {
        final int count = Integer.getInteger("usage.children", 20000);
        final int threadCount = Integer.getInteger("usage.threads", 16);

        final MemoryUsage[] children = new MemoryUsage[count];
        for (int i = 0; i < count; i++) {
            children[i] = new MemoryUsage(parent, "usage-storm-child-" + i, 1.0f);
        }

        final long addMillis = runPhase("addChild", threadCount, count,
            index -> children[index].start());
        assertEquals(count, parent.getChildren().size());

        final long removeMillis = runPhase("removeChild", threadCount, count,
            index -> children[index].stop());
        assertEquals(0, parent.getChildren().size());

        recordBenchResult(String.format(
            "[%s] Usage child storm (%d children, %d threads): addChild=%dms (%.2f us/op), "
            + "removeChild=%dms (%.2f us/op)",
            System.getProperty("usage.label", "run"), count, threadCount,
            addMillis, addMillis * 1000.0 / count,
            removeMillis, removeMillis * 1000.0 / count));
    }

    /**
     * Surefire report settings can discard test stdout, so benchmark results
     * are also appended to target/usage-child-bench.txt.
     */
    private static void recordBenchResult(String line) throws IOException {
        System.out.println(line);
        Path out = Paths.get("target", "usage-child-bench.txt");
        Files.createDirectories(out.getParent());
        Files.write(out, (line + System.lineSeparator()).getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
