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
package org.apache.activemq.broker;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(BlockJUnit4ClassRunner.class)
public class NioQueueSubscriptionTest extends QueueSubscriptionTest {

    protected static final Logger LOG = LoggerFactory.getLogger(NioQueueSubscriptionTest.class);

    private final Map<Thread, Throwable> exceptions = Collections.synchronizedMap(new HashMap<Thread, Throwable>());

    @Override
    protected ConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("tcp://localhost:62621?trace=false");
    }


    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService answer = BrokerFactory.createBroker(new URI(
            "broker://nio://localhost:62621?useQueueForAccept=false&persistent=false&wiewformat.maxInactivityDuration=0"));
        answer.getManagementContext().setCreateConnector(false);
        answer.setUseJmx(false);
        answer.setDeleteAllMessagesOnStartup(true);
        final List<PolicyEntry> policyEntries = new ArrayList<PolicyEntry>();
        final PolicyEntry entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setOptimizedDispatch(true);
        policyEntries.add(entry);

        final PolicyMap policyMap = new PolicyMap();
        policyMap.setPolicyEntries(policyEntries);
        answer.setDestinationPolicy(policyMap);
        return answer;
    }


    @Ignore("See AMQ-4286")
    @Test(timeout = 60 * 1000)
    public void testLotsOfConcurrentConnections() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        final ConnectionFactory factory = createConnectionFactory();
        int connectionCount = 400;
        final AtomicInteger threadId = new AtomicInteger(0);
        for (int i = 0; i < connectionCount; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    final int innerId = threadId.incrementAndGet();
                    try {
                        ExceptionListener listener = new NioQueueSubscriptionTestListener(innerId, exceptions, LOG);
                        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
                        connection.setExceptionListener(listener);
                        connection.start();
                        assertNotNull(connection.getBrokerName());
                        connections.add(connection);
                    } catch (Exception e) {
                        LOG.error(">>>> Exception in run() on thread " + innerId, e);
                        exceptions.put(Thread.currentThread(), e);
                    }
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        if (!exceptions.isEmpty()) {
            LOG.error(">>>> " + exceptions.size() + " exceptions like", exceptions.values().iterator().next());
            fail("unexpected exceptions in worker threads: " + exceptions.values().iterator().next());
        }
        LOG.info("created " + connectionCount + " connections");
    }
}

class NioQueueSubscriptionTestListener implements ExceptionListener {
    private int id = 0;
    protected Logger LOG;
    private final Map<Thread, Throwable> exceptions;

    public NioQueueSubscriptionTestListener(int id, Map<Thread, Throwable> exceptions, Logger log) {
        this.id = id;
        this.exceptions = exceptions;
        this.LOG = log;
    }

    @Override
    public void onException(JMSException exception) {
        LOG.error(">>>> Exception in onException() on thread " + id, exception);
        exceptions.put(Thread.currentThread(), exception);
    }
}
