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
import static org.junit.Assert.assertNotNull;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.failover.FailoverTransport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Every way of asking for virtual thread task runners, combined, with the
 * resulting field on each object that acts on the answer:
 * <ul>
 * <li>the connection factory, set by setter or by a {@code jms.} URL option,
 *     last write wins;</li>
 * <li>the connection, which copies the factory;</li>
 * <li>the failover transport, where a URI value is final, a factory {@code true}
 *     fills a gap, and nothing at all falls back to the system property;</li>
 * <li>the two task runner factories, whose executors also honour the system
 *     property on their own.</li>
 * </ul>
 */
@Category(ParallelTest.class)
@RunWith(Parameterized.class)
public class VirtualThreadConfigurationMatrixTest {

    /** how the connection factory flag is set */
    enum FactorySetting {
        UNSET, SETTER_TRUE, SETTER_FALSE, URI_TRUE, URI_FALSE, URI_TRUE_THEN_SETTER_FALSE
    }

    private static final String PROPERTY = TaskRunnerFactory.USE_VIRTUAL_THREAD_TASK_RUNNER_PROPERTY;
    private static BrokerService broker;

    @Parameterized.Parameters(name = "property={0} factory={1} transportUri={2}")
    public static Collection<Object[]> matrix() {
        List<Object[]> rows = new ArrayList<>();
        for (boolean property : new boolean[] {false, true}) {
            for (FactorySetting factory : FactorySetting.values()) {
                for (Boolean transportUri : new Boolean[] {null, Boolean.TRUE, Boolean.FALSE}) {
                    rows.add(new Object[] {property, factory, transportUri});
                }
            }
        }
        return rows;
    }

    @Parameterized.Parameter(0)
    public boolean property;

    @Parameterized.Parameter(1)
    public FactorySetting factory;

    @Parameterized.Parameter(2)
    public Boolean transportUri;

    private String previousProperty;

    @BeforeClass
    public static void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.start();
        broker.waitUntilStarted();
    }

    @AfterClass
    public static void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Before
    public void setProperty() {
        previousProperty = System.getProperty(PROPERTY);
        if (property) {
            System.setProperty(PROPERTY, "true");
        } else {
            System.clearProperty(PROPERTY);
        }
    }

    @After
    public void restoreProperty() {
        if (previousProperty == null) {
            System.clearProperty(PROPERTY);
        } else {
            System.setProperty(PROPERTY, previousProperty);
        }
    }

    @Test(timeout = 30000)
    public void fieldsOnEveryObjectFollowThePrecedence() throws Exception {
        var url = new StringBuilder("failover:(vm://localhost?create=false)?initialReconnectDelay=10&maxReconnectDelay=100");
        if (factory == FactorySetting.URI_TRUE || factory == FactorySetting.URI_TRUE_THEN_SETTER_FALSE) {
            url.append("&jms.useVirtualThreadTaskRunner=true");
        } else if (factory == FactorySetting.URI_FALSE) {
            url.append("&jms.useVirtualThreadTaskRunner=false");
        }
        if (transportUri != null) {
            url.append("&useVirtualThreadTaskRunner=").append(transportUri);
        }

        var connectionFactory = new ActiveMQConnectionFactory(url.toString());
        if (factory == FactorySetting.SETTER_TRUE) {
            connectionFactory.setUseVirtualThreadTaskRunner(true);
        } else if (factory == FactorySetting.SETTER_FALSE || factory == FactorySetting.URI_TRUE_THEN_SETTER_FALSE) {
            connectionFactory.setUseVirtualThreadTaskRunner(false);
        }

        boolean expectedFactory = factory == FactorySetting.SETTER_TRUE || factory == FactorySetting.URI_TRUE;
        assertEquals("connection factory field", expectedFactory, connectionFactory.isUseVirtualThreadTaskRunner());

        try (var connection = (ActiveMQConnection) connectionFactory.createConnection()) {
            connection.start();
            assertEquals("connection field copies the factory", expectedFactory, connection.isUseVirtualThreadTaskRunner());

            // the transport: a URI value is final, a factory true fills a gap, otherwise the property
            var failover = connection.getTransport().narrow(FailoverTransport.class);
            Boolean expectedRecorded = transportUri != null ? transportUri : (expectedFactory ? Boolean.TRUE : null);
            boolean expectedTransport = transportUri != null ? transportUri : (expectedFactory || property);
            assertEquals("value recorded on the transport", expectedRecorded, failover.getUseVirtualThreadTaskRunner());
            assertEquals("effective transport setting", expectedTransport, failover.isUseVirtualThreadTaskRunner());

            // start() creates the reconnect runner, so its factory carries the transport's answer
            var reconnectFactory = (TaskRunnerFactory) field(FailoverTransport.class, "reconnectTaskFactory").get(failover);
            assertNotNull("reconnect task factory created on start", reconnectFactory);
            assertEquals("reconnect factory flag", expectedTransport, reconnectFactory.isVirtualThreadTaskRunner());
            // TaskRunnerFactory.init() applies the system property on its own, so the executor is
            // virtual whenever either the flag or the property says so
            assertEquals("reconnect executor", expectedTransport || property, runsOnVirtualThread(reconnectFactory.getExecutor()));

            var sessionFactory = connection.getSessionTaskRunner();
            sessionFactory.init();
            assertEquals("session factory flag", expectedFactory, sessionFactory.isVirtualThreadTaskRunner());
            assertEquals("session executor", expectedFactory || property, runsOnVirtualThread(sessionFactory.getExecutor()));
        }
    }

    /**
     * Asks the executor itself: a task submitted to it reports whether it ran on
     * a virtual thread. Thread.isVirtual() is read reflectively because this
     * module compiles its tests for an older release.
     */
    private static boolean runsOnVirtualThread(ExecutorService executor) throws Exception {
        assertNotNull("executor initialised", executor);
        return executor.submit(() -> (Boolean) Thread.class.getMethod("isVirtual").invoke(Thread.currentThread()))
                .get(10, TimeUnit.SECONDS);
    }

    private static Field field(Class<?> type, String name) throws NoSuchFieldException {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }
}
