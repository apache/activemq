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

import junit.framework.Test;
import org.apache.activemq.JmsMultipleClientsTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.AbortSlowConsumerStrategyViewMBean;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.broker.region.policy.AbortSlowConsumerStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.util.MessageIdList;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.management.InstanceNotFoundException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;


public class AbortSlowConsumerBase extends JmsMultipleClientsTestSupport implements ExceptionListener {

    private static final Logger LOG = LoggerFactory.getLogger(AbortSlowConsumerBase.class);

    protected AbortSlowConsumerStrategy underTest;
    protected boolean abortConnection = false;
    protected long checkPeriod = 2 * 1000;
    protected long maxSlowDuration = 5 * 1000;
    protected final List<Throwable> exceptions = new ArrayList<Throwable>();

    @Override
    @Before
    public void setUp() throws Exception {
        exceptions.clear();
        topic = true;
        underTest = createSlowConsumerStrategy();
        super.setUp();
        createDestination();
    }

    protected AbortSlowConsumerStrategy createSlowConsumerStrategy() {
        return new AbortSlowConsumerStrategy();
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

    @Override
    public void onException(JMSException exception) {
        exceptions.add(exception);
        exception.printStackTrace();
    }

}
