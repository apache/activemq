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
package org.apache.activemq.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.management.ObjectName;

import org.apache.activemq.broker.jmx.AbortSlowConsumerStrategyViewMBean;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.ConnectionViewMBean;
import org.apache.activemq.broker.jmx.ConnectorViewMBean;
import org.apache.activemq.broker.jmx.DurableSubscriptionViewMBean;
import org.apache.activemq.broker.jmx.HealthViewMBean;
import org.apache.activemq.broker.jmx.JobSchedulerViewMBean;
import org.apache.activemq.broker.jmx.Log4JConfigViewMBean;
import org.apache.activemq.broker.jmx.NetworkBridgeViewMBean;
import org.apache.activemq.broker.jmx.NetworkConnectorViewMBean;
import org.apache.activemq.broker.jmx.PersistenceAdapterViewMBean;
import org.apache.activemq.broker.jmx.ProducerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.RecoveredXATransactionViewMBean;
import org.apache.activemq.broker.jmx.TopicSubscriptionViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.broker.jmx.VirtualDestinationSelectorCacheViewMBean;
import org.apache.activemq.plugin.jmx.RuntimeConfigurationViewMBean;
import org.apache.activemq.security.SecurityAdminMBean;
import org.apache.activemq.transport.TransportLoggerControlMBean;
import org.apache.activemq.transport.TransportLoggerViewMBean;
import org.jolokia.server.core.restrictor.policy.PolicyRestrictor;
import org.jolokia.server.core.util.HttpMethod;
import org.jolokia.server.core.util.RequestType;
import org.junit.Before;
import org.junit.Test;

/**
 * Evaluates the shipped jolokia-access.xml with Jolokia's own policy restrictor.
 * Jolokia consults &lt;allow&gt; only for command types missing from &lt;commands&gt; and
 * &lt;deny&gt; only for the ones present, so reading the file is not enough to tell
 * what it permits.
 */
public class JolokiaAccessPolicyTest {

    private static final String BROKER = "org.apache.activemq:type=Broker,brokerName=localhost";
    private static final String QUEUE = BROKER + ",destinationType=Queue,destinationName=TEST";
    private static final String TOPIC = BROKER + ",destinationType=Topic,destinationName=TEST";
    private static final String SUBSCRIPTION = QUEUE + ",endpoint=Consumer,clientId=client,consumerId=consumer";
    private static final String CONNECTOR = BROKER + ",connector=clientConnectors,connectorName=openwire";
    private static final String NETWORK_CONNECTOR = BROKER + ",connector=networkConnectors,networkConnectorName=nc";

    // The only operations reachable through Jolokia by default: none of them changes broker state.
    private static final Set<String> READ_ONLY_OPERATIONS = Set.of(
            "queryQueues", "queryTopics", "browseQueue", "getTransportConnectorByType",
            "browse", "browseAsTable", "getMessage",
            "cursorSize", "doesCursorHaveMessagesBuffered", "doesCursorHaveSpace",
            "isMatchingQueue", "isMatchingTopic",
            "health", "healthList", "healthStatus",
            "getAllJobs", "getNextScheduleJobs", "getExecutionCount",
            "getLogLevel", "connectionCount");

    private static final Map<String, Class<?>> MBEANS = new LinkedHashMap<>();
    static {
        MBEANS.put(BROKER, BrokerViewMBean.class);
        MBEANS.put(QUEUE, QueueViewMBean.class);
        MBEANS.put(TOPIC, TopicViewMBean.class);
        MBEANS.put(SUBSCRIPTION, DurableSubscriptionViewMBean.class);
        MBEANS.put(TOPIC + ",endpoint=Consumer,clientId=client,consumerId=consumer", TopicSubscriptionViewMBean.class);
        MBEANS.put(QUEUE + ",endpoint=Producer,clientId=client,producerId=producer", ProducerViewMBean.class);
        MBEANS.put(BROKER + ",service=Health", HealthViewMBean.class);
        MBEANS.put(BROKER + ",service=JobScheduler,name=JMS", JobSchedulerViewMBean.class);
        MBEANS.put(BROKER + ",service=Log4JConfiguration", Log4JConfigViewMBean.class);
        MBEANS.put(BROKER + ",service=PersistenceAdapter,instanceName=KahaDB", PersistenceAdapterViewMBean.class);
        MBEANS.put(BROKER + ",service=SlowConsumerStrategy,instanceName=strategy", AbortSlowConsumerStrategyViewMBean.class);
        MBEANS.put(BROKER + ",service=RuntimeConfiguration,name=XBean", RuntimeConfigurationViewMBean.class);
        MBEANS.put(BROKER + ",service=plugin,virtualDestinationSelectoCache=cache", VirtualDestinationSelectorCacheViewMBean.class);
        MBEANS.put(BROKER + ",transactionType=RecoveredXaTransaction,xid=xid", RecoveredXATransactionViewMBean.class);
        MBEANS.put(CONNECTOR, ConnectorViewMBean.class);
        MBEANS.put(CONNECTOR + ",connectionViewType=clientId,connectionName=client", ConnectionViewMBean.class);
        MBEANS.put(NETWORK_CONNECTOR, NetworkConnectorViewMBean.class);
        MBEANS.put(NETWORK_CONNECTOR + ",networkBridge=bridge", NetworkBridgeViewMBean.class);
        MBEANS.put("org.apache.activemq:type=Broker,brokerName=localhost,service=SecurityAdmin", SecurityAdminMBean.class);
        MBEANS.put("org.apache.activemq:type=Broker,brokerName=localhost,service=TransportLoggerControl", TransportLoggerControlMBean.class);
        MBEANS.put("org.apache.activemq:type=Broker,brokerName=localhost,service=TransportLogger", TransportLoggerViewMBean.class);
    }

    private PolicyRestrictor restrictor;

    @Before
    public void loadPolicy() throws Exception {
        try (InputStream in = new FileInputStream("src/release/conf/jolokia-access.xml")) {
            restrictor = new PolicyRestrictor(in);
        }
    }

    @Test
    public void onlyReadCommandsAreEnabled() {
        assertTrue(restrictor.isTypeAllowed(RequestType.READ));
        assertTrue(restrictor.isTypeAllowed(RequestType.LIST));
        assertTrue(restrictor.isTypeAllowed(RequestType.SEARCH));
        assertTrue(restrictor.isTypeAllowed(RequestType.VERSION));
        assertFalse(restrictor.isTypeAllowed(RequestType.WRITE));
        assertFalse(restrictor.isTypeAllowed(RequestType.EXEC));
        assertTrue(restrictor.isHttpMethodAllowed(HttpMethod.POST));
        assertFalse(restrictor.isHttpMethodAllowed(HttpMethod.GET));
    }

    @Test
    public void onlyReadOnlyOperationsCanBeExecuted() throws Exception {
        int allowed = 0;
        for (Map.Entry<String, Class<?>> mbean : MBEANS.entrySet()) {
            ObjectName name = new ObjectName(mbean.getKey());
            for (Method method : mbean.getValue().getMethods()) {
                if (isAttribute(method)) {
                    continue;
                }
                boolean expected = READ_ONLY_OPERATIONS.contains(method.getName());
                // a client may send the bare name or the name with its signature
                for (String operation : new String[] {method.getName(), signature(method)}) {
                    assertEquals(operation + " on " + name, expected, restrictor.isOperationAllowed(name, operation));
                }
                if (expected) {
                    allowed++;
                }
            }
        }
        assertTrue("no read-only operation was checked", allowed > 0);
    }

    @Test
    public void operationsOutsideTheBrokerDomainCannotBeExecuted() throws Exception {
        assertFalse(restrictor.isOperationAllowed(new ObjectName("java.lang:type=Memory"), "gc"));
        assertFalse(restrictor.isOperationAllowed(new ObjectName("java.lang:type=Threading"), "dumpAllThreads"));
        assertFalse(restrictor.isOperationAllowed(new ObjectName("com.sun.management:type=DiagnosticCommand"), "vmSystemProperties"));
        assertFalse(restrictor.isOperationAllowed(new ObjectName("com.sun.management:type=HotSpotDiagnostic"), "dumpHeap"));
        assertFalse(restrictor.isOperationAllowed(new ObjectName("javax.management.loading:type=MLet"), "getMBeansFromURL"));
    }

    @Test
    public void attributesCannotBeWritten() throws Exception {
        assertFalse(restrictor.isAttributeWriteAllowed(new ObjectName(BROKER), "MemoryLimit"));
        assertFalse(restrictor.isAttributeWriteAllowed(new ObjectName(QUEUE), "MaxPageSize"));
        assertFalse(restrictor.isAttributeWriteAllowed(new ObjectName(NETWORK_CONNECTOR), "Password"));
        assertFalse(restrictor.isAttributeWriteAllowed(new ObjectName("java.lang:type=Memory"), "Verbose"));
    }

    @Test
    public void attributesCanBeReadExceptSensitiveOnes() throws Exception {
        assertTrue(restrictor.isAttributeReadAllowed(new ObjectName(BROKER), "BrokerVersion"));
        assertTrue(restrictor.isAttributeReadAllowed(new ObjectName(QUEUE), "QueueSize"));
        assertFalse(restrictor.isAttributeReadAllowed(new ObjectName(NETWORK_CONNECTOR), "Password"));
        assertFalse(restrictor.isAttributeReadAllowed(new ObjectName(NETWORK_CONNECTOR), "RemotePassword"));
        assertFalse(restrictor.isAttributeReadAllowed(new ObjectName("java.lang:type=Runtime"), "SystemProperties"));
    }

    @Test
    public void onlyTheConsoleOriginsPassTheOriginCheck() {
        for (String origin : new String[] {"http://localhost:8161", "http://127.0.0.1:8161", "http://[::1]:8161",
                "https://localhost:8443", "https://127.0.0.1:8443", "https://[::1]:8443"}) {
            assertTrue(origin, restrictor.isOriginAllowed(origin, true));
        }
        for (String origin : new String[] {"https://example.org", "http://localhost:9999",
                "http://localhost.example.org:8161", "http://localhost:8161.example.org",
                "https://example.org/http://localhost:8161"}) {
            assertFalse(origin, restrictor.isOriginAllowed(origin, true));
            assertFalse(origin, restrictor.isOriginAllowed(origin, false));
        }
        // neither an Origin nor a Referer header
        assertFalse(restrictor.isOriginAllowed(null, true));
    }

    // JMX exposes getters and setters as attributes, everything else as operations
    private static boolean isAttribute(Method method) {
        String name = method.getName();
        int parameters = method.getParameterCount();
        Class<?> returnType = method.getReturnType();
        return (name.startsWith("get") && parameters == 0 && returnType != void.class)
                || (name.startsWith("is") && parameters == 0 && returnType == boolean.class)
                || (name.startsWith("set") && parameters == 1 && returnType == void.class);
    }

    private static String signature(Method method) {
        return method.getName() + Arrays.stream(method.getParameterTypes())
                .map(Class::getName)
                .collect(Collectors.joining(",", "(", ")"));
    }
}
