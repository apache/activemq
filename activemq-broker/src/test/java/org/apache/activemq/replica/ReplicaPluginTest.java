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
import org.assertj.core.api.SoftAssertions;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;

public class ReplicaPluginTest {

    private final ReplicaPlugin plugin = new ReplicaPlugin();

    @Test
    public void canSetRole() {
        SoftAssertions softly = new SoftAssertions();
        Arrays.stream(ReplicaRole.values()).forEach(role -> {

            softly.assertThat(plugin.setRole(role)).isSameAs(plugin);
            softly.assertThat(plugin.getRole()).isEqualTo(role);

            plugin.setRole(role.name());
            softly.assertThat(plugin.getRole()).isEqualTo(role);
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

        assertThat(plugin.otherBrokerConnectionFactory).isNotNull()
                .extracting(ActiveMQConnectionFactory::getBrokerURL)
                .isEqualTo("failover:(tcp://localhost:61616)");
    }

    @Test
    public void canSetOtherBrokerUriFluently() {
        ReplicaPlugin result = plugin.connectedTo(URI.create("failover:(tcp://localhost:61616)"));

        assertThat(result).isSameAs(plugin);
        assertThat(result.otherBrokerConnectionFactory).isNotNull()
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

        assertThat(plugin.otherBrokerConnectionFactory).isNotNull()
                .extracting(ActiveMQConnectionFactory::getBrokerURL)
                .isEqualTo("failover:(tcp://localhost:61616)");
    }

    @Test
    public void canSetTransportConnectorUri() {
        plugin.setTransportConnectorUri("tcp://0.0.0.0:61618?maximumConnections=1&amp;wireFormat.maxFrameSize=104857600");

        assertThat(plugin.transportConnectorUri).isNotNull()
                .isEqualTo(URI.create("tcp://0.0.0.0:61618?maximumConnections=1&amp;wireFormat.maxFrameSize=104857600"));
    }

    @Test
    public void rejectsInvalidTransportConnectorUri() {
        Throwable expected = catchThrowable(() -> URI.create("inval:{id}-uri"));

        Throwable exception = catchThrowable(() -> plugin.setTransportConnectorUri("inval:{id}-uri"));

        assertThat(exception).isNotNull().isEqualToComparingFieldByField(expected);
    }

    @Test
    public void canSetUserName() {
        final String userName = "testUser";

        plugin.setUserName(userName);

        assertThat(plugin.otherBrokerConnectionFactory.getUserName()).isEqualTo(userName);
    }

    @Test
    public void canSetPassword() {
        final String password = "testPassword";

        plugin.setPassword(password);

        assertThat(plugin.otherBrokerConnectionFactory.getPassword()).isEqualTo(password);
    }

    @Test
    public void canSetUserNameAndPassword() {
        final String userUsername = "testUser";
        final String password = "testPassword";

        plugin.setUserName(userUsername);
        plugin.setPassword(password);

        assertThat(plugin.otherBrokerConnectionFactory.getUserName()).isEqualTo(userUsername);
        assertThat(plugin.otherBrokerConnectionFactory.getPassword()).isEqualTo(password);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionIfUserIsSetAndPasswordIsNotForReplica() {
        final String userName = "testUser";
        final Broker broker = mock(Broker.class);
        final String replicationTransport = "tcp://localhost:61616";

        plugin.setRole(ReplicaRole.replica);
        plugin.setUserName(userName);
        plugin.setTransportConnectorUri(replicationTransport);
        plugin.installPlugin(broker);

        assertThat(plugin.otherBrokerConnectionFactory.getUserName()).isEqualTo(userName);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionIfPasswordIsSetAndUserNameIsNotForReplica() {
        final String password = "testPassword";
        final Broker broker = mock(Broker.class);
        final String replicationTransport = "tcp://localhost:61616";

        plugin.setRole(ReplicaRole.replica);
        plugin.setPassword(password);
        plugin.setTransportConnectorUri(replicationTransport);
        plugin.installPlugin(broker);

        assertThat(plugin.otherBrokerConnectionFactory.getPassword()).isEqualTo(password);
    }

    @Test
    public void shouldNotThrowExceptionIfBothUserAndPasswordIsSetForReplica() {
        final String user = "testUser";
        final String password = "testPassword";
        final Broker broker = mock(Broker.class);
        final String replicationTransport = "tcp://localhost:61616";

        plugin.setRole(ReplicaRole.replica);
        plugin.setPassword(password);
        plugin.setUserName(user);
        plugin.setTransportConnectorUri(replicationTransport);
        plugin.installPlugin(broker);

        assertThat(plugin.otherBrokerConnectionFactory.getUserName()).isEqualTo(user);
        assertThat(plugin.otherBrokerConnectionFactory.getPassword()).isEqualTo(password);
        assertThat(plugin.otherBrokerConnectionFactory.getPrefetchPolicy().getQueuePrefetch()).isEqualTo(plugin.prefetchLimit);
    }

    @Test
    public void canSetPrefetchLimit() {
        final String user = "testUser";
        final String password = "testPassword";
        final Broker broker = mock(Broker.class);
        final String replicationTransport = "tcp://localhost:61616";
        final int prefetchLimit = 100;

        plugin.setRole(ReplicaRole.replica);
        plugin.setPassword(password);
        plugin.setUserName(user);
        plugin.setTransportConnectorUri(replicationTransport);
        plugin.setPrefetchLimit(prefetchLimit);
        plugin.installPlugin(broker);

        assertThat(plugin.otherBrokerConnectionFactory.getPrefetchPolicy().getQueuePrefetch()).isEqualTo(prefetchLimit);
    }


}
