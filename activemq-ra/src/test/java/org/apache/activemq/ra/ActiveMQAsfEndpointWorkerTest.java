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
package org.apache.activemq.ra;

import javax.jms.Connection;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.endpoint.MessageEndpointFactory;

import org.jmock.Mock;
import org.jmock.cglib.MockObjectTestCase;

/**
 * @author <a href="mailto:michael.gaffney@panacya.com">Michael Gaffney </a>
 */
public class ActiveMQAsfEndpointWorkerTest extends MockObjectTestCase {

    private Mock mockResourceAdapter;
    private Mock mockActivationKey;
    private Mock mockEndpointFactory;
    private Mock mockBootstrapContext;
    private ActiveMQActivationSpec stubActivationSpec;
//    private Mock mockConnection;

    public ActiveMQAsfEndpointWorkerTest(String name) {
        setName(name);
    }

    public void testTopicSubscriberDurableNoDups() throws Exception {
//         Constraint[] args = {isA(Topic.class),
//            eq(stubActivationSpec.getSubscriptionId()), 
//            NULL, 
//            ANYTHING,
//            ANYTHING}; 
//         mockConnection.expects(once()).method("createDurableConnectionConsumer").with(args)
//            .will(returnValue(null)); 
//         worker.start();
//         verifyMocks();
    }

    protected void setUp() throws Exception {
        setupStubs();
        setupMocks();
        setupEndpointWorker();
    }

    private void setupStubs() {
        stubActivationSpec = new ActiveMQActivationSpec();
        stubActivationSpec.setDestination("some.topic");
        stubActivationSpec.setDestinationType("javax.jms.Topic");
        stubActivationSpec.setSubscriptionDurability(ActiveMQActivationSpec.DURABLE_SUBSCRIPTION);
        stubActivationSpec.setClientId("foo");
        stubActivationSpec.setSubscriptionName("bar");
    }

    private void setupMocks() {
        mockResourceAdapter = mock(ActiveMQResourceAdapter.class);
        mockActivationKey = mock(ActiveMQEndpointActivationKey.class);
        mockEndpointFactory = mock(MessageEndpointFactory.class);
        mockBootstrapContext = mock(BootstrapContext.class);
//        mockConnection = mock(Connection.class);

        mockActivationKey.expects(atLeastOnce()).method("getMessageEndpointFactory").will(returnValue((MessageEndpointFactory)mockEndpointFactory.proxy()));

        mockActivationKey.expects(atLeastOnce()).method("getActivationSpec").will(returnValue(stubActivationSpec));

        mockResourceAdapter.expects(atLeastOnce()).method("getBootstrapContext").will(returnValue((BootstrapContext)mockBootstrapContext.proxy()));

        mockBootstrapContext.expects(atLeastOnce()).method("getWorkManager").will(returnValue(null));

        final boolean isTransactedResult = true;
        setupIsTransacted(isTransactedResult);
    }

    private void setupIsTransacted(final boolean transactedResult) {
        mockEndpointFactory.expects(atLeastOnce()).method("isDeliveryTransacted").with(ANYTHING).will(returnValue(transactedResult));
    }

    private void setupEndpointWorker() throws Exception {
        new ActiveMQEndpointWorker((ActiveMQResourceAdapter)mockResourceAdapter.proxy(), (ActiveMQEndpointActivationKey)mockActivationKey.proxy());
    }

//    private void verifyMocks() {
//        mockResourceAdapter.verify();
//        mockActivationKey.verify();
//        mockEndpointFactory.verify();
//        mockBootstrapContext.verify();
//        mockConnection.verify();
//    }

}
