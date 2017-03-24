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
package org.apache.activemq.security;

import java.util.Arrays;
import java.util.Set;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.CompositeDestinationInterceptor;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ProducerInfo;

/**
 * Verifies if a authenticated user can do an operation against the broker using
 * an authorization map.
 *
 *
 */
public class AuthorizationBroker extends BrokerFilter implements SecurityAdminMBean {

    private volatile AuthorizationMap authorizationMap;

    public AuthorizationBroker(Broker next, AuthorizationMap authorizationMap) {
        super(next);
        this.authorizationMap = authorizationMap;

        // add DestinationInterceptor
        final RegionBroker regionBroker = (RegionBroker) next.getAdaptor(RegionBroker.class);
        final CompositeDestinationInterceptor compositeInterceptor = (CompositeDestinationInterceptor) regionBroker.getDestinationInterceptor();
        DestinationInterceptor[] interceptors = compositeInterceptor.getInterceptors();
        interceptors = Arrays.copyOf(interceptors, interceptors.length + 1);
        interceptors[interceptors.length - 1] = new AuthorizationDestinationInterceptor(this);
        compositeInterceptor.setInterceptors(interceptors);
    }

    public AuthorizationMap getAuthorizationMap() {
        return authorizationMap;
    }

    public void setAuthorizationMap(AuthorizationMap map) {
        authorizationMap = map;
    }

    protected SecurityContext checkSecurityContext(ConnectionContext context) throws SecurityException {
        final SecurityContext securityContext = context.getSecurityContext();
        if (securityContext == null) {
            throw new SecurityException("User is not authenticated.");
        }
        return securityContext;
    }

    protected boolean checkDestinationAdmin(SecurityContext securityContext, ActiveMQDestination destination) {
        Destination existing = this.getDestinationMap(destination).get(destination);
        if (existing != null) {
            return true;
        }

        if (!securityContext.isBrokerContext()) {
            Set<?> allowedACLs = null;
            if (!destination.isTemporary()) {
                allowedACLs = authorizationMap.getAdminACLs(destination);
            } else {
                allowedACLs = authorizationMap.getTempDestinationAdminACLs();
            }

            if (allowedACLs != null && !securityContext.isInOneOf(allowedACLs)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        final SecurityContext securityContext = checkSecurityContext(context);

        if (!checkDestinationAdmin(securityContext, info.getDestination())) {
            throw new SecurityException("User " + securityContext.getUserName() + " is not authorized to create: " + info.getDestination());
        }

        super.addDestinationInfo(context, info);
    }

    @Override
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination,boolean create) throws Exception {
        final SecurityContext securityContext = checkSecurityContext(context);

        if (!checkDestinationAdmin(securityContext, destination)) {
            throw new SecurityException("User " + securityContext.getUserName() + " is not authorized to create: " + destination);
        }

        return super.addDestination(context, destination,create);
    }

    @Override
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        final SecurityContext securityContext = checkSecurityContext(context);

        if (!checkDestinationAdmin(securityContext, destination)) {
            throw new SecurityException("User " + securityContext.getUserName() + " is not authorized to remove: " + destination);
        }

        securityContext.getAuthorizedWriteDests().remove(destination);

        super.removeDestination(context, destination, timeout);
    }

    @Override
    public void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        final SecurityContext securityContext = checkSecurityContext(context);

        if (!checkDestinationAdmin(securityContext, info.getDestination())) {
            throw new SecurityException("User " + securityContext.getUserName() + " is not authorized to remove: " + info.getDestination());
        }

        securityContext.getAuthorizedWriteDests().remove(info.getDestination());

        super.removeDestinationInfo(context, info);
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        final SecurityContext securityContext = checkSecurityContext(context);

        Set<?> allowedACLs = null;
        if (!info.getDestination().isTemporary()) {
            allowedACLs = authorizationMap.getReadACLs(info.getDestination());
        } else {
            allowedACLs = authorizationMap.getTempDestinationReadACLs();
        }

        if (!securityContext.isBrokerContext() && allowedACLs != null && !securityContext.isInOneOf(allowedACLs) ) {
            throw new SecurityException("User " + securityContext.getUserName() + " is not authorized to read from: " + info.getDestination());
        }

        /*
         * Need to think about this a little more. We could do per message
         * security checking to implement finer grained security checking. For
         * example a user can only see messages with price>1000 . Perhaps this
         * should just be another additional broker filter that installs this
         * type of feature. If we did want to do that, then we would install a
         * predicate. We should be careful since there may be an existing
         * predicate already assigned and the consumer info may be sent to a
         * remote broker, so it also needs to support being marshaled.
         * info.setAdditionalPredicate(new BooleanExpression() { public boolean
         * matches(MessageEvaluationContext message) throws JMSException { if(
         * !subject.getAuthorizedReadDests().contains(message.getDestination()) ) {
         * Set allowedACLs =
         * authorizationMap.getReadACLs(message.getDestination());
         * if(allowedACLs!=null && !subject.isInOneOf(allowedACLs)) return
         * false; subject.getAuthorizedReadDests().put(message.getDestination(),
         * message.getDestination()); } return true; } public Object
         * evaluate(MessageEvaluationContext message) throws JMSException {
         * return matches(message) ? Boolean.TRUE : Boolean.FALSE; } });
         */

        return super.addConsumer(context, info);
    }

    @Override
    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        final SecurityContext securityContext = checkSecurityContext(context);

        if (!securityContext.isBrokerContext() && info.getDestination() != null) {

            Set<?> allowedACLs = null;
            if (!info.getDestination().isTemporary()) {
                allowedACLs = authorizationMap.getWriteACLs(info.getDestination());
            } else {
                allowedACLs = authorizationMap.getTempDestinationWriteACLs();
            }
            if (allowedACLs != null && !securityContext.isInOneOf(allowedACLs)) {
                throw new SecurityException("User " + securityContext.getUserName() + " is not authorized to write to: " + info.getDestination());
            }
            securityContext.getAuthorizedWriteDests().put(info.getDestination(), info.getDestination());
        }

        super.addProducer(context, info);
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        final SecurityContext securityContext = checkSecurityContext(producerExchange.getConnectionContext());

        if (!securityContext.isBrokerContext() && !securityContext.getAuthorizedWriteDests().containsValue(messageSend.getDestination())) {

            Set<?> allowedACLs = null;
            if (!messageSend.getDestination().isTemporary()) {
                allowedACLs = authorizationMap.getWriteACLs(messageSend.getDestination());
            } else {
                allowedACLs = authorizationMap.getTempDestinationWriteACLs();
            }

            if (allowedACLs != null && !securityContext.isInOneOf(allowedACLs)) {
                throw new SecurityException("User " + securityContext.getUserName() + " is not authorized to write to: " + messageSend.getDestination());
            }
            securityContext.getAuthorizedWriteDests().put(messageSend.getDestination(), messageSend.getDestination());
        }

        super.send(producerExchange, messageSend);
    }

    // SecurityAdminMBean interface
    // -------------------------------------------------------------------------

    @Override
    public void addQueueRole(String queue, String operation, String role) {
        addDestinationRole(new ActiveMQQueue(queue), operation, role);
    }

    @Override
    public void addTopicRole(String topic, String operation, String role) {
        addDestinationRole(new ActiveMQTopic(topic), operation, role);
    }

    @Override
    public void removeQueueRole(String queue, String operation, String role) {
        removeDestinationRole(new ActiveMQQueue(queue), operation, role);
    }

    @Override
    public void removeTopicRole(String topic, String operation, String role) {
        removeDestinationRole(new ActiveMQTopic(topic), operation, role);
    }

    public void addDestinationRole(javax.jms.Destination destination, String operation, String role) {
    }

    public void removeDestinationRole(javax.jms.Destination destination, String operation, String role) {
    }

    @Override
    public void addRole(String role) {
    }

    @Override
    public void addUserRole(String user, String role) {
    }

    @Override
    public void removeRole(String role) {
    }

    @Override
    public void removeUserRole(String user, String role) {
    }

}
