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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.network.DemandForwardingBridgeSupport.FutureBrokerInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies that {@link DemandForwardingBridgeSupport.FutureBrokerInfo}
 * honors the timeout passed to {@link FutureBrokerInfo#get(long, TimeUnit)}.
 *
 * The timed get loop must exit when EITHER the bridge is disposed OR the
 * deadline expires. A faulty OR condition on the two checks keeps the loop
 * alive as long as the bridge is not disposed, ignoring the caller's timeout
 * entirely and parking bridge start threads indefinitely when the peer never
 * delivers its BrokerInfo.
 */
public class FutureBrokerInfoTimeoutTest {

    private ExecutorService executor;

    @Before
    public void setUp() {
        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown() {
        // interrupts a worker left parked by a get() that ignored its timeout
        executor.shutdownNow();
    }

    /**
     * No info, not disposed: get(200ms) must throw TimeoutException promptly
     * rather than blocking until disposal.
     */
    @Test(timeout = 10000)
    public void testGetTimedThrowsTimeoutExceptionWithinTimeout() throws Exception {
        var disposed = new AtomicBoolean(false);
        var future = new FutureBrokerInfo(null, disposed);

        // returns the elapsed time when the expected TimeoutException is
        // thrown, -1 when get() returned without one
        var elapsed = executor.submit(() -> {
            var start = System.currentTimeMillis();
            try {
                future.get(200, TimeUnit.MILLISECONDS);
                return -1L;
            } catch (TimeoutException expected) {
                return System.currentTimeMillis() - start;
            }
        });

        try {
            var elapsedMillis = elapsed.get(3, TimeUnit.SECONDS);
            assertTrue("Expected TimeoutException from get(200ms)", elapsedMillis >= 0);
            assertTrue("Timed out too early: " + elapsedMillis + "ms", elapsedMillis >= 180);
        } catch (TimeoutException stillBlocked) {
            disposed.set(true);
            fail("get(200ms) should have returned within 3s but is still blocked "
                    + "- the timeout is being ignored");
        }
    }

    /**
     * Already disposed, no info: get with a long timeout must not wait out the
     * full timeout - disposal exits the wait immediately and surfaces as
     * TimeoutException (info is absent).
     */
    @Test(timeout = 10000)
    public void testGetTimedExitsPromptlyWhenAlreadyDisposed() throws Exception {
        var disposed = new AtomicBoolean(true);
        var future = new FutureBrokerInfo(null, disposed);

        var timedOut = executor.submit(() -> {
            try {
                future.get(60, TimeUnit.SECONDS);
                return false;
            } catch (TimeoutException expected) {
                return true;
            }
        });

        try {
            assertTrue("Expected TimeoutException on disposed future with no info",
                    timedOut.get(3, TimeUnit.SECONDS));
        } catch (TimeoutException stillBlocked) {
            fail("get(60s) on a disposed future should return immediately "
                    + "- it must not wait out the deadline");
        }
    }

    /**
     * Happy path: info already present - get returns it regardless of timeout.
     */
    @Test(timeout = 10000)
    public void testGetTimedReturnsInfoWhenAlreadySet() throws Exception {
        var disposed = new AtomicBoolean(false);
        var future = new FutureBrokerInfo(null, disposed);

        var brokerInfo = new BrokerInfo();
        brokerInfo.setBrokerName("test-broker");
        future.set(brokerInfo);

        var result = future.get(200, TimeUnit.MILLISECONDS);
        assertNotNull(result);
        assertEquals("test-broker", result.getBrokerName());
    }

    /**
     * Info arrives mid-wait: get returns it promptly, well before the timeout.
     */
    @Test(timeout = 10000)
    public void testGetTimedReturnsPromptlyWhenInfoSetDuringWait() throws Exception {
        var disposed = new AtomicBoolean(false);
        var future = new FutureBrokerInfo(null, disposed);

        var brokerInfo = new BrokerInfo();
        brokerInfo.setBrokerName("late-broker");

        var result = executor.submit(() -> future.get(30, TimeUnit.SECONDS));

        Thread.sleep(100);
        future.set(brokerInfo);

        try {
            var info = result.get(3, TimeUnit.SECONDS);
            assertNotNull(info);
            assertEquals("late-broker", info.getBrokerName());
        } catch (TimeoutException stillBlocked) {
            fail("get should have returned promptly after set()");
        }
    }
}
