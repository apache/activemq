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
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.CompositeDestinationInterceptor;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.security.SecurityContext;

import java.util.Arrays;

public class ReplicaAuthorizationBroker extends BrokerFilter implements MutativeRoleBroker {

    public ReplicaAuthorizationBroker(Broker next) {
        super(next);
        // add DestinationInterceptor
        final RegionBroker regionBroker = (RegionBroker) next.getAdaptor(RegionBroker.class);
        final CompositeDestinationInterceptor compositeInterceptor = (CompositeDestinationInterceptor) regionBroker.getDestinationInterceptor();
        DestinationInterceptor[] interceptors = compositeInterceptor.getInterceptors();
        interceptors = Arrays.copyOf(interceptors, interceptors.length + 1);
        interceptors[interceptors.length - 1] = new ReplicaDestinationInterceptor();
        compositeInterceptor.setInterceptors(interceptors);
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        assertAuthorized(context, info.getDestination());
        return super.addConsumer(context, info);
    }

    @Override
    public void addProducer(ConnectionContext context, ProducerInfo producerInfo) throws Exception {
        // JMS allows producers to be created without first specifying a destination.  In these cases, every send
        // operation must specify a destination.  Because of this, we only authorize 'addProducer' if a destination is
        // specified. If not specified, the authz check in the 'send' method below will ensure authorization.
        if (producerInfo.getDestination() != null) {
            assertAuthorized(context, producerInfo.getDestination());
        }
        super.addProducer(context, producerInfo);
    }

    @Override
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        if (ReplicaSupport.isReplicationQueue(destination) && !ReplicaSupport.isInternalUser(context.getUserName()) ) {
            throw new ActiveMQReplicaException(createUnauthorizedMessage(destination));
        }
        super.removeDestination(context, destination, timeout);
    }

    @Override
    public void stopBeforeRoleChange(boolean force) throws Exception {
        ((MutativeRoleBroker) next).stopBeforeRoleChange(force);
    }

    @Override
    public void startAfterRoleChange() throws Exception {
        ((MutativeRoleBroker) next).startAfterRoleChange();
    }

    @Override
    public void initializeRoleChangeCallBack(ActionListenerCallback actionListenerCallback) {
        ((MutativeRoleBroker) next).initializeRoleChangeCallBack(actionListenerCallback);
    }

    private void assertAuthorized(ConnectionContext context, ActiveMQDestination destination) {
        if (isAuthorized(context, destination)) {
            return;
        }

        throw new ActiveMQReplicaException(createUnauthorizedMessage(destination));
    }

    private static boolean isAuthorized(ConnectionContext context, ActiveMQDestination destination) {
        boolean replicationQueue = ReplicaSupport.isReplicationQueue(destination);
        boolean replicationTransport = ReplicaSupport.isReplicationTransport(context.getConnector());

        if (isSystemBroker(context)) {
            return true;
        }
        if (replicationTransport && (replicationQueue || ReplicaSupport.isAdvisoryDestination(destination))) {
            return true;
        }
        if (!replicationTransport && !replicationQueue) {
            return true;
        }
        return false;
    }

    private static boolean isSystemBroker(ConnectionContext context) {
        SecurityContext securityContext = context.getSecurityContext();
        return securityContext != null && securityContext.isBrokerContext();
    }

    private static String createUnauthorizedMessage(ActiveMQDestination destination) {
        return "Not authorized to access destination: " + destination;
    }

    private static class ReplicaDestinationInterceptor implements DestinationInterceptor {

        @Override
        public Destination intercept(Destination destination) {
            if (ReplicaSupport.isReplicationQueue(destination.getActiveMQDestination())) {
                return new ReplicaDestinationFilter(destination);
            }
            return destination;
        }

        @Override
        public void remove(Destination destination) {
        }

        @Override
        public void create(Broker broker, ConnectionContext context, ActiveMQDestination destination) throws Exception {
        }
    }

    private static class ReplicaDestinationFilter extends DestinationFilter {


        public ReplicaDestinationFilter(Destination next) {
            super(next);
        }

        @Override
        public void addSubscription(ConnectionContext context, Subscription sub) throws Exception {
            if (!isAuthorized(context, getActiveMQDestination())) {
                throw new SecurityException(createUnauthorizedMessage(getActiveMQDestination()));
            }
            super.addSubscription(context, sub);
        }
    }
}
