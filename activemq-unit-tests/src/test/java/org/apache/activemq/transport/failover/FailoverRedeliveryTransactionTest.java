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
package org.apache.activemq.transport.failover;

import junit.framework.Test;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;

public class FailoverRedeliveryTransactionTest extends FailoverTransactionTest {

    public static Test suite() {
        return suite(FailoverRedeliveryTransactionTest.class);
    }

    @Override
    public void configureConnectionFactory(ActiveMQConnectionFactory factory) {
        super.configureConnectionFactory(factory);
        factory.setTransactedIndividualAck(true);
    }

    @Override
    public BrokerService createBroker(boolean deleteAllMessagesOnStartup, String bindAddress) throws Exception {
        BrokerService brokerService = super.createBroker(deleteAllMessagesOnStartup, bindAddress);
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setPersistJMSRedelivered(true);
        policyMap.setDefaultEntry(defaultEntry);
        brokerService.setDestinationPolicy(policyMap);
        return brokerService;
    }

    // no point rerunning these
    @Override
    public void testFailoverProducerCloseBeforeTransaction() throws Exception {
    }

    @Override
    public void initCombosForTestFailoverCommitReplyLost() {
    }

    @Override
    public void testFailoverCommitReplyLost() throws Exception {
    }

    @Override
    public void testFailoverCommitReplyLostWithDestinationPathSeparator() throws Exception {
    }

    @Override
    public void initCombosForTestFailoverSendReplyLost() {
    }

    @Override
    public void testFailoverSendReplyLost() throws Exception {
    }

    @Override
    public void initCombosForTestFailoverConnectionSendReplyLost() {
    }

    @Override
    public void testFailoverConnectionSendReplyLost() throws Exception {
    }

    @Override
    public void testFailoverProducerCloseBeforeTransactionFailWhenDisabled() throws Exception {
    }

    @Override
    public void testFailoverMultipleProducerCloseBeforeTransaction() throws Exception {
    }
}
