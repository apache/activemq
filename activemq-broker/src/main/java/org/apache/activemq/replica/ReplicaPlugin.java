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

import org.apache.activemq.advisory.AdvisoryBroker;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.MutableBrokerFilter;
import org.apache.activemq.broker.jmx.AnnotatedMBean;
import org.apache.activemq.broker.region.CompositeDestinationInterceptor;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.replica.jmx.ReplicationJmxHelper;
import org.apache.activemq.replica.jmx.ReplicationView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A Broker plugin to replicate core messaging events from one broker to another.
 *
 * @org.apache.xbean.XBean element="replicaPlugin"
 */
public class ReplicaPlugin extends BrokerPluginSupport {

    private final Logger logger = LoggerFactory.getLogger(ReplicaPlugin.class);

    protected ReplicaRole role = ReplicaRole.source;

    protected ReplicaPolicy replicaPolicy = new ReplicaPolicy();

    private ReplicationView replicationView;

    private ReplicaRoleManagementBroker replicaRoleManagementBroker;

    public ReplicaPlugin() {
        super();
    }

    @Override
    public Broker installPlugin(final Broker broker) throws Exception {
        if (role != ReplicaRole.source && role != ReplicaRole.replica) {
            throw new IllegalArgumentException(String.format("Unsupported role [%s]", role.name()));
        }

        logger.info("{} installed, running as {}", ReplicaPlugin.class.getName(), role);

        final BrokerService brokerService = broker.getBrokerService();
        if (brokerService.isUseJmx()) {
            replicationView = new ReplicationView(this);
            AnnotatedMBean.registerMBean(brokerService.getManagementContext(), replicationView, ReplicationJmxHelper.createJmxName(brokerService));
        }
        
        List<PolicyEntry> policyEntries = new ArrayList<>();
        for (String queue : ReplicaSupport.REPLICATION_QUEUE_NAMES) {
            PolicyEntry newPolicy = getPolicyEntry(new ActiveMQQueue(queue));
            newPolicy.setMaxPageSize(ReplicaSupport.INTERMEDIATE_QUEUE_PREFETCH_SIZE);
            policyEntries.add(newPolicy);
        }
        for (String topic : ReplicaSupport.REPLICATION_TOPIC_NAMES) {
            policyEntries.add(getPolicyEntry(new ActiveMQTopic(topic)));
        }
        if (brokerService.getDestinationPolicy() == null) {
            brokerService.setDestinationPolicy(new PolicyMap());
        }
        brokerService.getDestinationPolicy().setPolicyEntries(policyEntries);

        RegionBroker regionBroker = (RegionBroker) broker.getAdaptor(RegionBroker.class);
        CompositeDestinationInterceptor compositeInterceptor = (CompositeDestinationInterceptor) regionBroker.getDestinationInterceptor();
        DestinationInterceptor[] interceptors = compositeInterceptor.getInterceptors();
        interceptors = Arrays.copyOf(interceptors, interceptors.length + 1);
        interceptors[interceptors.length - 1] = new ReplicaAdvisorySuppressor();
        compositeInterceptor.setInterceptors(interceptors);

        replicaRoleManagementBroker = new ReplicaRoleManagementBroker(broker, replicaPolicy, role);

        return new ReplicaAuthorizationBroker(replicaRoleManagementBroker);
    }

    private void addInterceptor4CompositeQueues(final Broker broker, final Broker sourceBroker, final ReplicaRoleManagementBroker roleManagementBroker) {
        final RegionBroker regionBroker = (RegionBroker) broker.getAdaptor(RegionBroker.class);
        final CompositeDestinationInterceptor compositeInterceptor = (CompositeDestinationInterceptor) regionBroker.getDestinationInterceptor();
        DestinationInterceptor[] interceptors = compositeInterceptor.getInterceptors();
        interceptors = Arrays.copyOf(interceptors, interceptors.length + 1);
        interceptors[interceptors.length - 1] = new ReplicaDestinationInterceptor((ReplicaSourceBroker)sourceBroker, roleManagementBroker);
        compositeInterceptor.setInterceptors(interceptors);
    }

    private MutativeRoleBroker buildReplicaBroker(Broker broker, ReplicaFailOverStateStorage replicaFailOverStateStorage, WebConsoleAccessController webConsoleAccessController) {
        return new ReplicaBroker(broker, queueProvider, replicaPolicy, replicaFailOverStateStorage, webConsoleAccessController);
    }

    private MutativeRoleBroker buildSourceBroker(Broker broker, ReplicaFailOverStateStorage replicaFailOverStateStorage, WebConsoleAccessController webConsoleAccessController) {
        ReplicaInternalMessageProducer replicaInternalMessageProducer =
                new ReplicaInternalMessageProducer(broker);
        ReplicationMessageProducer replicationMessageProducer =
                new ReplicationMessageProducer(replicaInternalMessageProducer, queueProvider);

        ReplicaSequencer replicaSequencer = new ReplicaSequencer(broker, queueProvider, replicaInternalMessageProducer,
                replicationMessageProducer, replicaPolicy);

        return new ReplicaSourceBroker(broker, replicationMessageProducer, replicaSequencer,
                        queueProvider, replicaPolicy, replicaFailOverStateStorage, webConsoleAccessController);
    }

    private PolicyEntry getPolicyEntry(ActiveMQDestination destination) {
        PolicyEntry newPolicy = new PolicyEntry();
        newPolicy.setGcInactiveDestinations(false);
        newPolicy.setDestination(destination);
        return newPolicy;
    }

    public ReplicaPlugin setRole(ReplicaRole role) {
        this.role = requireNonNull(role);
        return this;
    }

    public ReplicaPlugin connectedTo(URI uri) {
        this.setOtherBrokerUri(requireNonNull(uri).toString());
        return this;
    }

    /**
     * @org.apache.xbean.Property propertyEditor="com.sun.beans.editors.StringEditor"
     */
    public void setRole(String role) {
        this.role = Arrays.stream(ReplicaRole.values())
            .filter(roleValue -> roleValue.name().equalsIgnoreCase(role))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(role + " is not a known " + ReplicaRole.class.getSimpleName()));
    }

    /**
     * @org.apache.xbean.Property propertyEditor="com.sun.beans.editors.StringEditor"
     */
    public void setOtherBrokerUri(String uri) {
        replicaPolicy.setOtherBrokerUri(uri);
    }

    /**
     * @org.apache.xbean.Property propertyEditor="com.sun.beans.editors.StringEditor"
     */
    public void setTransportConnectorUri(String uri) {
        replicaPolicy.setTransportConnectorUri(URI.create(uri));
    }

    /**
     * @org.apache.xbean.Property propertyEditor="com.sun.beans.editors.StringEditor"
     */
    public void setUserName(String userName) {
        replicaPolicy.setUserName(userName);
    }

    /**
     * @org.apache.xbean.Property propertyEditor="com.sun.beans.editors.StringEditor"
     */
    public void setPassword(String password) {
        replicaPolicy.setPassword(password);
    }

    /**
     * @org.apache.xbean.Property propertyEditor="com.sun.beans.editors.StringEditor"
     */
    public void setSourceSendPeriod(int period) {
        replicaPolicy.setSourceSendPeriod(period);
    }

    /**
     * @org.apache.xbean.Property propertyEditor="com.sun.beans.editors.StringEditor"
     */
    public void setCompactorAdditionalMessagesLimit(int limit) {
        replicaPolicy.setCompactorAdditionalMessagesLimit(limit);
    }

    /**
     * @org.apache.xbean.Property propertyEditor="com.sun.beans.editors.StringEditor"
     */
    public void setMaxBatchLength(int length) {
        replicaPolicy.setMaxBatchLength(length);
    }

    /**
     * @org.apache.xbean.Property propertyEditor="com.sun.beans.editors.StringEditor"
     */
    public void setMaxBatchSize(int size) {
        replicaPolicy.setMaxBatchSize(size);
    }

    /**
     * @org.apache.xbean.Property propertyEditor="com.sun.beans.editors.StringEditor"
     */
    public void setReplicaAckPeriod(int period) {
        replicaPolicy.setReplicaAckPeriod(period);
    }

    /**
     * @org.apache.xbean.Property propertyEditor="com.sun.beans.editors.StringEditor"
     */
    public void setReplicaMaxAckBatchSize(int size) {
        replicaPolicy.setReplicaMaxAckBatchSize(size);
    }

    /**
     * @org.apache.xbean.Property propertyEditor="com.sun.beans.editors.StringEditor"
     */
    public void setControlWebConsoleAccess(boolean controlWebConsoleAccess) {
        replicaPolicy.setControlWebConsoleAccess(controlWebConsoleAccess);
    }

    public ReplicaRole getRole() {
        return replicaRoleManagementBroker.getRole().getExternalRole();
    }

    public void setReplicaRole(ReplicaRole role, boolean force) throws Exception {
        logger.info("Called switch role for broker. Params: [{}], [{}]", role.name(), force);

        if (role != ReplicaRole.replica && role != ReplicaRole.source) {
            throw new RuntimeException(String.format("Can't switch role from [%s] to [%s]", this.role.name(), role.name()));
        }

        replicaRoleManagementBroker.switchRole(role, force);
    }
}
