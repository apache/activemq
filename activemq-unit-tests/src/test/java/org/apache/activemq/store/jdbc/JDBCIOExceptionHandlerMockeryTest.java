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
package org.apache.activemq.store.jdbc;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.Locker;
import org.apache.activemq.broker.SuppressReplyException;
import org.apache.activemq.util.LeaseLockerIOExceptionHandler;
import org.apache.activemq.util.Wait;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCIOExceptionHandlerMockeryTest {
    private static final Logger LOG = LoggerFactory.getLogger(JDBCIOExceptionHandlerMockeryTest.class);
    private HashMap<Thread, Throwable> exceptions = new HashMap<Thread, Throwable>();

    @Test
    public void testShutdownWithoutTransportRestart() throws Exception {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            LOG.error("unexpected exception {} on thread {}", e, t);
            exceptions.put(t, e);
        });

        // Create mocks
        BrokerService brokerService = mock(BrokerService.class);
        JDBCPersistenceAdapter jdbcPersistenceAdapter = mock(JDBCPersistenceAdapter.class);
        Locker locker = mock(Locker.class);

        // Setup mock behaviors
        when(brokerService.isStarted()).thenReturn(true);
        when(brokerService.isRestartAllowed()).thenReturn(false);
        when(brokerService.getPersistenceAdapter()).thenReturn(jdbcPersistenceAdapter);
        when(jdbcPersistenceAdapter.getLocker()).thenReturn(locker);
        when(locker.keepAlive()).thenReturn(true);  // Connection is down

        LeaseLockerIOExceptionHandler underTest = new LeaseLockerIOExceptionHandler();
        underTest.setBrokerService(brokerService);

        try {
            underTest.handle(new IOException());
            fail("except suppress reply ex");
        } catch (SuppressReplyException expected) {
        }


        assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());
    }
}
