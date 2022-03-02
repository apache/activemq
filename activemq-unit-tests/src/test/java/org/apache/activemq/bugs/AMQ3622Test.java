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
package org.apache.activemq.bugs;

import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.FilePendingSubscriberMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.LastImageSubscriptionRecoveryPolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.layout.MessageLayout;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AMQ3622Test {

    protected static final Appender appender;
    protected static final AtomicBoolean failed = new AtomicBoolean(false);
    protected BrokerService broker;
    protected String connectionUri;
    protected Logger logger;

    static {
       appender = new AbstractAppender("testAppender", new AbstractFilter() {}, new MessageLayout(), false, new Property[0]) {
        @Override
        public void append(LogEvent event) {
            System.err.println(event.getMessage());
            if (event.getThrown() != null) {
                if (event.getThrown() instanceof NullPointerException) {
                    failed.set(true);
                }
            }
            }
        };
        appender.start();
    }

    @Before
    public void before() throws Exception {

        logger = Logger.class.cast(LogManager.getRootLogger());
        logger.get().addAppender(appender, Level.DEBUG, new AbstractFilter() {});

        broker = new BrokerService();
        broker.setDataDirectory("target" + File.separator + "activemq-data");
        broker.setPersistent(true);
        broker.setDeleteAllMessagesOnStartup(true);
        PolicyEntry policy = new PolicyEntry();
        policy.setTopic(">");
        policy.setProducerFlowControl(false);
        policy.setMemoryLimit(1 * 1024 * 1024);
        policy.setPendingSubscriberPolicy(new FilePendingSubscriberMessageStoragePolicy());
        policy.setSubscriptionRecoveryPolicy(new LastImageSubscriptionRecoveryPolicy());
        policy.setExpireMessagesPeriod(500);
        List<PolicyEntry> entries = new ArrayList<PolicyEntry>();

        entries.add(policy);
        PolicyMap pMap = new PolicyMap();
        pMap.setPolicyEntries(entries);
        broker.setDestinationPolicy(pMap);

        connectionUri = broker.addConnector("stomp://localhost:0").getPublishableConnectString();

        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void after() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
        logger.removeAppender(appender);
    }

    @Test
    public void go() throws Exception {
        StompConnection connection = new StompConnection();
        Integer port = Integer.parseInt(connectionUri.split(":")[2]);
        connection.open("localhost", port);        
        connection.connect("", "");
        connection.subscribe("/topic/foobar", Stomp.Headers.Subscribe.AckModeValues.CLIENT);
        connection.disconnect();
        Thread.sleep(1000);

        if (failed.get()) {
            fail("Received NullPointerException");
        }
    }

}
