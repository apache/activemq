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

import java.io.IOException;
import java.util.HashMap;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.Locker;
import org.apache.activemq.broker.SuppressReplyException;
import org.apache.activemq.util.LeaseLockerIOExceptionHandler;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.Wait;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.States;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JDBCIOExceptionHandlerMockeryTest {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCIOExceptionHandlerMockeryTest.class);
    private HashMap<Thread, Throwable> exceptions = new HashMap<Thread, Throwable>();

    @Test
    public void testShutdownWithoutTransportRestart() throws Exception {

        Mockery context = new Mockery() {{
            setImposteriser(ClassImposteriser.INSTANCE);
        }};

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error("unexpected exception {} on thread {}", e, t);
                exceptions.put(t, e);
            }
        });

        final BrokerService brokerService = context.mock(BrokerService.class);
        final JDBCPersistenceAdapter jdbcPersistenceAdapter = context.mock(JDBCPersistenceAdapter.class);
        final Locker locker = context.mock(Locker.class);

        final States jdbcConn = context.states("jdbc").startsAs("down");
        final States broker = context.states("broker").startsAs("started");

        // simulate jdbc up between hasLock and checkpoint, so hasLock fails to verify
        context.checking(new Expectations() {{
            allowing(brokerService).isStarted();
            will(returnValue(true));
            allowing(brokerService).isRestartAllowed();
            will(returnValue(false));
            allowing(brokerService).setSystemExitOnShutdown(with(false));
            allowing(brokerService).stopAllConnectors(with(any(ServiceStopper.class)));
            allowing(brokerService).getPersistenceAdapter();
            will(returnValue(jdbcPersistenceAdapter));
            allowing(jdbcPersistenceAdapter).allowIOResumption();
            allowing(jdbcPersistenceAdapter).getLocker();
            will(returnValue(locker));
            allowing(locker).keepAlive();
            when(jdbcConn.is("down"));
            will(returnValue(true));
            allowing(locker).keepAlive();
            when(jdbcConn.is("up"));
            will(returnValue(false));

            allowing(jdbcPersistenceAdapter).checkpoint(with(true));
            then(jdbcConn.is("up"));
            allowing(brokerService).stop();
            then(broker.is("stopped"));

        }});

        LeaseLockerIOExceptionHandler underTest = new LeaseLockerIOExceptionHandler();
        underTest.setBrokerService(brokerService);

        try {
            underTest.handle(new IOException());
            fail("except suppress reply ex");
        } catch (SuppressReplyException expected) {
        }

        assertTrue("broker stopped state triggered", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("broker state {}", broker);
                return broker.is("stopped").isActive();
            }
        }));
        context.assertIsSatisfied();

        assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());
    }
}
