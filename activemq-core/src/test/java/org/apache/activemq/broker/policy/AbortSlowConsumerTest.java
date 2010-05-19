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
package org.apache.activemq.broker.policy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import junit.framework.Test;

import org.apache.activemq.JmsMultipleClientsTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.AbortSlowConsumerStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.util.MessageIdList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class AbortSlowConsumerTest extends JmsMultipleClientsTestSupport implements ExceptionListener {

    private static final Log LOG = LogFactory.getLog(AbortSlowConsumerTest.class);
    
    AbortSlowConsumerStrategy underTest;
    
    public boolean abortConnection = false;
    public long checkPeriod = 2*1000;
    public long maxSlowDuration = 5*1000;

    private List<Throwable> exceptions = new ArrayList<Throwable>();
    
    @Override
    protected void setUp() throws Exception {
        exceptions.clear();
        topic = true;
        underTest = new AbortSlowConsumerStrategy();
        super.setUp();
        createDestination();
    }
    
    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();
        PolicyEntry policy = new PolicyEntry();
        underTest.setAbortConnection(abortConnection);
        underTest.setCheckPeriod(checkPeriod);
        underTest.setMaxSlowDuration(maxSlowDuration);

        policy.setSlowConsumerStrategy(underTest);
        policy.setQueuePrefetch(10);
        policy.setTopicPrefetch(10);
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(pMap);
        return broker;
    }

    public void testRegularConsumerIsNotAborted() throws Exception {
        startConsumers(destination);
        for (Connection c: connections) {
            c.setExceptionListener(this);
        }
        startProducers(destination, 100);
        allMessagesList.waitForMessagesToArrive(10);
        allMessagesList.assertAtLeastMessagesReceived(10);
    }

    public void initCombosForTestLittleSlowConsumerIsNotAborted() {
        addCombinationValues("topic", new Object[]{Boolean.TRUE, Boolean.FALSE});
    }
    
    public void testLittleSlowConsumerIsNotAborted() throws Exception {
        startConsumers(destination);
        Entry<MessageConsumer, MessageIdList> consumertoAbort = consumers.entrySet().iterator().next();
        consumertoAbort.getValue().setProcessingDelay(500);
        for (Connection c: connections) {
            c.setExceptionListener(this);
        }
        startProducers(destination, 12);
        allMessagesList.waitForMessagesToArrive(10);
        allMessagesList.assertAtLeastMessagesReceived(10);
    }

    
    public void initCombosForTestSlowConsumerIsAborted() {
        addCombinationValues("abortConnection", new Object[]{Boolean.TRUE, Boolean.FALSE});
        addCombinationValues("topic", new Object[]{Boolean.TRUE, Boolean.FALSE});
    }
    
    public void testSlowConsumerIsAborted() throws Exception {
        startConsumers(destination);
        Entry<MessageConsumer, MessageIdList> consumertoAbort = consumers.entrySet().iterator().next();
        consumertoAbort.getValue().setProcessingDelay(8*1000);
        for (Connection c: connections) {
            c.setExceptionListener(this);
        }
        startProducers(destination, 100);
        
        consumertoAbort.getValue().assertMessagesReceived(1);
     
        TimeUnit.SECONDS.sleep(5);
        
        consumertoAbort.getValue().assertAtMostMessagesReceived(1);        
    }

    
    public void testOnlyOneSlowConsumerIsAborted() throws Exception {
        consumerCount = 10;
        startConsumers(destination);
        Entry<MessageConsumer, MessageIdList> consumertoAbort = consumers.entrySet().iterator().next();
        consumertoAbort.getValue().setProcessingDelay(8*1000);
        for (Connection c: connections) {
            c.setExceptionListener(this);
        }
        startProducers(destination, 100);
        
        allMessagesList.waitForMessagesToArrive(99);
        allMessagesList.assertAtLeastMessagesReceived(99);
        
        consumertoAbort.getValue().assertMessagesReceived(1);
     
        TimeUnit.SECONDS.sleep(5);
        
        consumertoAbort.getValue().assertAtMostMessagesReceived(1);        
    }
    
    public void testAbortAlreadyClosingConsumers() throws Exception {
        consumerCount = 1;
        startConsumers(destination);
        for (MessageIdList list : consumers.values()) {
            list.setProcessingDelay(6*1000);
        }
        for (Connection c: connections) {
            c.setExceptionListener(this);
        }
        startProducers(destination, 100);
        allMessagesList.waitForMessagesToArrive(consumerCount);

        for (MessageConsumer consumer : consumers.keySet()) {
            LOG.info("closing consumer: " + consumer);
            /// will block waiting for on message till 6secs expire
            consumer.close();
        }
    }
    
    public void initCombosForTestAbortAlreadyClosedConsumers() {
        addCombinationValues("abortConnection", new Object[]{Boolean.TRUE, Boolean.FALSE});
        addCombinationValues("topic", new Object[]{Boolean.TRUE, Boolean.FALSE});
    }
    
    public void testAbortAlreadyClosedConsumers() throws Exception {
        Connection conn = createConnectionFactory().createConnection();
        conn.setExceptionListener(this);
        connections.add(conn);

        Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        final MessageConsumer consumer = sess.createConsumer(destination);
        conn.start();
        startProducers(destination, 20);
        TimeUnit.SECONDS.sleep(1);
        LOG.info("closing consumer: " + consumer);
        consumer.close();
        
        TimeUnit.SECONDS.sleep(5);
        assertTrue("no exceptions : " + exceptions.toArray(), exceptions.isEmpty());
    }

    
    public void initCombosForTestAbortAlreadyClosedConnection() {
        addCombinationValues("abortConnection", new Object[]{Boolean.TRUE, Boolean.FALSE});
        addCombinationValues("topic", new Object[]{Boolean.TRUE, Boolean.FALSE});
    }
    
    public void testAbortAlreadyClosedConnection() throws Exception {
        Connection conn = createConnectionFactory().createConnection();
        conn.setExceptionListener(this);

        Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        sess.createConsumer(destination);
        conn.start();
        startProducers(destination, 20);
        TimeUnit.SECONDS.sleep(1);
        LOG.info("closing connection: " + conn);
        conn.close();
        
        TimeUnit.SECONDS.sleep(5);
        assertTrue("no exceptions : " + exceptions.toArray(), exceptions.isEmpty());
    }

    public void testAbortConsumerOnDeadConnection() throws Exception {
        // socket proxy on pause, close could hang??
    }
    
    public void onException(JMSException exception) {
        exceptions.add(exception);
        exception.printStackTrace();        
    }
    
    public static Test suite() {
        return suite(AbortSlowConsumerTest.class);
    }
}
