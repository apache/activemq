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

package org.apache.activemq.util;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SuppressReplyException;
import org.junit.Test;


import static org.junit.Assert.*;

public class DefaultIOExceptionHandlerTest {

    DefaultIOExceptionHandler underTest = new DefaultIOExceptionHandler();

    @Test
    public void testHandleWithShutdownOnExit() throws Exception {
        doTest(true);
    }

    @Test
    public void testHandleWithOutShutdownOnExit() throws Exception {
        doTest(false);
    }

    protected void doTest(boolean exitPlease) throws Exception {
        final CountDownLatch stopCalled = new CountDownLatch(1);
        final AtomicBoolean shutdownOnExitSet = new AtomicBoolean(false);

        underTest.setSystemExitOnShutdown(exitPlease);
        underTest.setBrokerService(new BrokerService() {
            @Override
            public boolean isStarted() {
                return true;
            }

            @Override
            public void stop() throws Exception {
                shutdownOnExitSet.set(isSystemExitOnShutdown());
                stopCalled.countDown();
                // ensure we don't actually exit the jvm
                setSystemExitOnShutdown(false);
                super.stop();
            }
        });

        try {
            underTest.handle(new IOException("cause stop"));
            fail("Expect suppress reply exception");
        } catch (SuppressReplyException expected) {}

        assertTrue("stop called on time", stopCalled.await(10, TimeUnit.SECONDS));
        assertEquals("exit on shutdown set", exitPlease, shutdownOnExitSet.get());
    }
}