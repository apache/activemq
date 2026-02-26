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
package org.apache.activemq.tck;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JNDI InitialContextFactory for the Jakarta Messaging 3.1.0 TCK.
 * <p>
 * Starts an embedded ActiveMQ broker (non-persistent, no JMX) on first lookup
 * and provides the JNDI names required by the TCK test harness.
 */
public class JNDIInitialContextFactory implements InitialContextFactory {

    private static final Logger LOG = LoggerFactory.getLogger(JNDIInitialContextFactory.class);

    private static final String BROKER_URL = "vm://localhost";

    private static volatile BrokerService broker;
    private static final Object BROKER_LOCK = new Object();

    private static final Set<String> QUEUE_NAMES = Set.of(
        "MY_QUEUE", "MY_QUEUE2",
        "testQ0", "testQ1", "testQ2",
        "testQueue2", "Q2"
    );

    private static final Set<String> TOPIC_NAMES = Set.of(
        "MY_TOPIC", "MY_TOPIC2",
        "testT0", "testT1", "testT2"
    );

    private static final Set<String> CONNECTION_FACTORY_NAMES = Set.of(
        "MyConnectionFactory",
        "MyQueueConnectionFactory",
        "MyTopicConnectionFactory"
    );

    private static final String DURABLE_SUB_CF = "DURABLE_SUB_CONNECTION_FACTORY";

    @Override
    public Context getInitialContext(final Hashtable<?, ?> environment) throws NamingException {
        ensureBrokerStarted();

        final Map<String, Object> bindings = new ConcurrentHashMap<>();

        // Connection factories
        for (final String name : CONNECTION_FACTORY_NAMES) {
            bindings.put(name, createConnectionFactory(null));
        }
        bindings.put(DURABLE_SUB_CF, createConnectionFactory("cts"));

        // Queues
        for (final String name : QUEUE_NAMES) {
            bindings.put(name, new ActiveMQQueue(name));
        }

        // Topics
        for (final String name : TOPIC_NAMES) {
            bindings.put(name, new ActiveMQTopic(name));
        }

        return createContextProxy(bindings);
    }

    private static ActiveMQConnectionFactory createConnectionFactory(final String clientId) {
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        factory.setNestedMapAndListEnabled(false);
        if (clientId != null) {
            factory.setClientID(clientId);
        }
        return factory;
    }

    private static void ensureBrokerStarted() {
        if (broker != null) {
            return;
        }
        synchronized (BROKER_LOCK) {
            if (broker != null) {
                return;
            }
            try {
                final BrokerService bs = new BrokerService();
                bs.setBrokerName("localhost");
                bs.setPersistent(false);
                bs.setUseJmx(false);
                bs.setAdvisorySupport(false);
                bs.start();
                bs.waitUntilStarted();
                broker = bs;
                LOG.info("Embedded ActiveMQ broker started for TCK");

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        broker.stop();
                        broker.waitUntilStopped();
                    } catch (final Exception e) {
                        LOG.warn("Error stopping embedded broker", e);
                    }
                }, "activemq-tck-shutdown"));
            } catch (final Exception e) {
                throw new RuntimeException("Failed to start embedded ActiveMQ broker", e);
            }
        }
    }

    private static Context createContextProxy(final Map<String, Object> bindings) {
        return (Context) Proxy.newProxyInstance(
            JNDIInitialContextFactory.class.getClassLoader(),
            new Class<?>[]{Context.class},
            new ContextInvocationHandler(bindings)
        );
    }

    private static final class ContextInvocationHandler implements InvocationHandler {
        private final Map<String, Object> bindings;

        ContextInvocationHandler(final Map<String, Object> bindings) {
            this.bindings = bindings;
        }

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
            final String methodName = method.getName();

            if ("lookup".equals(methodName) && args != null && args.length == 1) {
                final String name = args[0] instanceof javax.naming.Name
                    ? ((javax.naming.Name) args[0]).toString()
                    : (String) args[0];
                final Object result = bindings.get(name);
                if (result == null) {
                    throw new NamingException("Name not found: " + name);
                }
                return result;
            }

            if ("close".equals(methodName)) {
                return null;
            }

            if ("toString".equals(methodName)) {
                return "ActiveMQ TCK JNDI Context" + bindings.keySet();
            }

            if ("hashCode".equals(methodName)) {
                return System.identityHashCode(proxy);
            }

            if ("equals".equals(methodName)) {
                return proxy == args[0];
            }

            throw new NamingException("Operation not supported: " + methodName);
        }
    }
}
