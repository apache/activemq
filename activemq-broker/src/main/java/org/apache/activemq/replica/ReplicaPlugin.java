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
import org.apache.activemq.broker.scheduler.SchedulerBroker;
import org.apache.activemq.replica.jmx.ReplicationView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.net.URI;
import java.util.Arrays;

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

    public ReplicaPlugin() {
        super();
    }

    @Override
    public Broker installPlugin(final Broker broker) throws Exception {
        logger.info("{} installed, running as {}", ReplicaPlugin.class.getName(), role);

        ReplicaReplicationQueueSupplier queueProvider = new ReplicaReplicationQueueSupplier(broker);

        final BrokerService brokerService = broker.getBrokerService();
        if (brokerService.isUseJmx()) {
            replicationView = new ReplicationView(this);
        }

        if (role == ReplicaRole.replica) {
            registerMBean(brokerService);
            return new ReplicaBroker(broker, queueProvider, replicaPolicy);
        }
        ReplicaInternalMessageProducer replicaInternalMessageProducer =
                new ReplicaInternalMessageProducer(broker);
        ReplicationMessageProducer replicationMessageProducer =
                new ReplicationMessageProducer(replicaInternalMessageProducer, queueProvider);

        ReplicaSequencer replicaSequencer = new ReplicaSequencer(broker, queueProvider, replicaInternalMessageProducer,
                replicationMessageProducer, replicaPolicy);

        ReplicaSourceBroker replicaSourceBroker = new ReplicaSourceBroker(broker, replicationMessageProducer, replicaSequencer,
                queueProvider, replicaPolicy);
        ReplicaSourceAuthorizationBroker replicaSourceAuthorizationBroker = new ReplicaSourceAuthorizationBroker(
                replicaSourceBroker);

        Broker replicaBrokerFilter;
        switch (role) {
            case source:
                replicaBrokerFilter = replicaSourceAuthorizationBroker;
                break;
            case dual:
                replicaBrokerFilter = new ReplicaBroker(replicaSourceAuthorizationBroker, queueProvider, replicaPolicy);
                break;
            default:
                throw new IllegalArgumentException();
        }

        MutableBrokerFilter scheduledBroker = (MutableBrokerFilter) broker.getAdaptor(SchedulerBroker.class);
        if (scheduledBroker != null) {
            scheduledBroker.setNext(new ReplicaSchedulerSourceBroker(scheduledBroker.getNext(), replicationMessageProducer));
        }

        MutableBrokerFilter advisoryBroker = (MutableBrokerFilter) broker.getAdaptor(AdvisoryBroker.class);
        if (advisoryBroker != null) {
            advisoryBroker.setNext(new ReplicaAdvisorySuppressor(advisoryBroker.getNext()));
        }

        registerMBean(brokerService);

        return replicaBrokerFilter;
    }

    private void registerMBean(BrokerService brokerService) throws Exception {
        if (brokerService.isUseJmx()) {
            AnnotatedMBean.registerMBean(brokerService.getManagementContext(), replicationView, createJmxName(brokerService));
        }
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

    public ReplicaRole getRole() {
        return role;
    }

    public void setReplicaRole(ReplicaRole role, boolean force) {
        logger.info("Called switch role for broker. Params: [{}], [{}]", role.name(), force);
    }

    private ObjectName createJmxName(BrokerService brokerService) {
        try {
            String objectNameStr = brokerService.getBrokerObjectName().toString();

            objectNameStr += "," + "service=Plugins";
            objectNameStr += "," + "instanceName=ReplicationPlugin";

            return new ObjectName(objectNameStr);
        } catch (MalformedObjectNameException e) {
            throw new RuntimeException("Failed to create JMX view for ReplicationPlugin", e);
        }
    }

}
