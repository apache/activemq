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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.CompositeDestinationInterceptor;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.SystemUsage;
import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReplicaPluginTest {

    private final ReplicaPlugin plugin = new ReplicaPlugin();

    @Before
    public void setUp() {
        plugin.setControlWebConsoleAccess(false);
    }

    @Test
    public void canSetRole() {
        SoftAssertions softly = new SoftAssertions();
        Arrays.stream(ReplicaRole.values()).forEach(role -> {

            softly.assertThat(plugin.setRole(role)).isSameAs(plugin);
            softly.assertThat(plugin.role).isEqualTo(role);

            plugin.setRole(role.name());
            softly.assertThat(plugin.role).isEqualTo(role);
        });
        softly.assertAll();
    }

    @Test
    public void rejectsUnknownRole() {
        Throwable exception = catchThrowable(() -> plugin.setRole("unknown"));

        assertThat(exception).isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("unknown is not a known " + ReplicaRole.class.getSimpleName());
    }

    @Test
    public void canSetOtherBrokerUri() {
        plugin.setOtherBrokerUri("failover:(tcp://localhost:61616)");

        assertThat(plugin.replicaPolicy.getOtherBrokerConnectionFactory()).isNotNull()
                .extracting(ActiveMQConnectionFactory::getBrokerURL)
                .isEqualTo("failover:(tcp://localhost:61616)");
    }

    @Test
    public void canSetOtherBrokerUriFluently() {
        ReplicaPlugin result = plugin.connectedTo(URI.create("failover:(tcp://localhost:61616)"));

        assertThat(result).isSameAs(plugin);
        assertThat(plugin.replicaPolicy.getOtherBrokerConnectionFactory()).isNotNull()
                .extracting(ActiveMQConnectionFactory::getBrokerURL)
                .isEqualTo("failover:(tcp://localhost:61616)");
    }

    @Test
    public void rejectsInvalidUnknownOtherBrokerUri() {
        Throwable expected = catchThrowable(() -> new ActiveMQConnectionFactory().setBrokerURL("inval:{id}-uri"));

        Throwable exception = catchThrowable(() -> plugin.setOtherBrokerUri("inval:{id}-uri"));

        assertThat(exception).isNotNull().isEqualToComparingFieldByField(expected);
    }

    @Test
    public void canSetOtherBrokerUriWithAutomaticAdditionOfFailoverTransport() {
        plugin.setOtherBrokerUri("tcp://localhost:61616");

        assertThat(plugin.replicaPolicy.getOtherBrokerConnectionFactory()).isNotNull()
                .extracting(ActiveMQConnectionFactory::getBrokerURL)
                .isEqualTo("failover:(tcp://localhost:61616)");
    }

    @Test
    public void canSetTransportConnectorUri() {
        plugin.setTransportConnectorUri("tcp://0.0.0.0:61618?maximumConnections=1&amp;wireFormat.maxFrameSize=104857600");

        assertThat(plugin.replicaPolicy.getTransportConnectorUri()).isNotNull()
                .isEqualTo(URI.create("tcp://0.0.0.0:61618?maximumConnections=1&amp;wireFormat.maxFrameSize=104857600"));
    }

    @Test
    public void rejectsInvalidTransportConnectorUri() {
        Throwable expected = catchThrowable(() -> URI.create("inval:{id}-uri"));

        Throwable exception = catchThrowable(() -> plugin.setTransportConnectorUri("inval:{id}-uri"));

        assertThat(exception).isNotNull().isEqualToComparingFieldByField(expected);
    }

    @Test
    public void canSetUserNameAndPassword() {
        String userUsername = "testUser";
        String password = "testPassword";

        plugin.setUserName(userUsername);
        plugin.setPassword(password);

        assertThat(plugin.replicaPolicy.getOtherBrokerConnectionFactory().getUserName()).isEqualTo(userUsername);
        assertThat(plugin.replicaPolicy.getOtherBrokerConnectionFactory().getPassword()).isEqualTo(password);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionIfUserIsSetAndPasswordIsNotForReplica() throws Exception {
        String userName = "testUser";
        Broker broker = mock(Broker.class);
        String replicationTransport = "tcp://localhost:61616";

        plugin.setRole(ReplicaRole.replica);
        plugin.setUserName(userName);
        plugin.setTransportConnectorUri(replicationTransport);
        plugin.installPlugin(broker);

        assertThat(plugin.replicaPolicy.getOtherBrokerConnectionFactory().getUserName()).isEqualTo(userName);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionIfPasswordIsSetAndUserNameIsNotForReplica() throws Exception {
        String password = "testPassword";
        Broker broker = mock(Broker.class);
        String replicationTransport = "tcp://localhost:61616";

        plugin.setRole(ReplicaRole.replica);
        plugin.setPassword(password);
        plugin.setTransportConnectorUri(replicationTransport);
        plugin.installPlugin(broker);

        assertThat(plugin.replicaPolicy.getOtherBrokerConnectionFactory().getPassword()).isEqualTo(password);
    }

    @Test
    public void shouldNotThrowExceptionIfBothUserAndPasswordIsSetForReplica() throws Exception {
        String user = "testUser";
        String password = "testPassword";
        Broker broker = mock(Broker.class);
        BrokerService brokerService = mock(BrokerService.class);
        when(brokerService.getDestinationPolicy()).thenReturn(new PolicyMap());
        when(broker.getBrokerService()).thenReturn(brokerService);
        when(brokerService.isUseJmx()).thenReturn(false);
        SystemUsage systemUsage = mock(SystemUsage.class);
        when(brokerService.getSystemUsage()).thenReturn(systemUsage);
        MemoryUsage memoryUsage = mock(MemoryUsage.class);
        when(systemUsage.getMemoryUsage()).thenReturn(memoryUsage);
        String replicationTransport = "tcp://localhost:61616";

        RegionBroker regionBroker = mock(RegionBroker.class);
        when(broker.getAdaptor(RegionBroker.class)).thenReturn(regionBroker);
        CompositeDestinationInterceptor cdi = mock(CompositeDestinationInterceptor.class);
        when(regionBroker.getDestinationInterceptor()).thenReturn(cdi);
        when(cdi.getInterceptors()).thenReturn(new DestinationInterceptor[]{});

        plugin.setRole(ReplicaRole.replica);
        plugin.setPassword(password);
        plugin.setUserName(user);
        plugin.setTransportConnectorUri(replicationTransport);
        plugin.installPlugin(broker);

        assertThat(plugin.replicaPolicy.getOtherBrokerConnectionFactory().getUserName()).isEqualTo(user);
        assertThat(plugin.replicaPolicy.getOtherBrokerConnectionFactory().getPassword()).isEqualTo(password);
    }
}
