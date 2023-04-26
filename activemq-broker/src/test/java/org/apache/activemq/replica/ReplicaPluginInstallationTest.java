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
package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.CompositeDestinationInterceptor;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ReplicaPluginInstallationTest {

    private final BrokerService brokerService = mock(BrokerService.class);
    private final Broker broker = mock(Broker.class);
    private final ReplicaPlugin pluginUnderTest = new ReplicaPlugin();

    @Before
    public void setUp() {
        pluginUnderTest.setControlWebConsoleAccess(false);
        when(broker.getBrokerService()).thenReturn(brokerService);
        when(brokerService.isUseJmx()).thenReturn(false);
        when(brokerService.getDestinationPolicy()).thenReturn(new PolicyMap());

        RegionBroker regionBroker = mock(RegionBroker.class);
        when(broker.getAdaptor(RegionBroker.class)).thenReturn(regionBroker);
        CompositeDestinationInterceptor cdi = mock(CompositeDestinationInterceptor.class);
        when(regionBroker.getDestinationInterceptor()).thenReturn(cdi);
        when(cdi.getInterceptors()).thenReturn(new DestinationInterceptor[]{});
    }

    @Test
    public void testInstallPluginWithDefaultRole() throws Exception {
        pluginUnderTest.setTransportConnectorUri("failover:(tcp://localhost:61616)");
        Broker installedBroker = pluginUnderTest.installPlugin(broker);
        assertThat(installedBroker).isInstanceOf(ReplicaAuthorizationBroker.class);
        Broker nextBroker = ((BrokerFilter) installedBroker).getNext();
        assertThat(nextBroker).isInstanceOf(ReplicaRoleManagementBroker.class);
        assertThat(((BrokerFilter) nextBroker).getNext()).isEqualTo(broker);
        assertThat(ReplicaRole.source).isEqualTo(pluginUnderTest.getRole());
    }

    @Test
    public void testInstallPluginWithReplicaRole() throws Exception {
        pluginUnderTest.setRole(ReplicaRole.replica);
        pluginUnderTest.setOtherBrokerUri("failover:(tcp://localhost:61616)");
        Broker installedBroker = pluginUnderTest.installPlugin(broker);
        assertThat(installedBroker).isInstanceOf(ReplicaAuthorizationBroker.class);
        Broker nextBroker = ((BrokerFilter) installedBroker).getNext();
        assertThat(nextBroker).isInstanceOf(ReplicaRoleManagementBroker.class);

        assertThat(((BrokerFilter) nextBroker).getNext()).isEqualTo(broker);
    }
}
