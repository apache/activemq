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

import java.util.Set;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;

/**
 * Authorizes addSubscription calls.
 */
public class AuthorizationDestinationFilter extends DestinationFilter {

    private final AuthorizationBroker broker;

    public AuthorizationDestinationFilter(Destination destination, AuthorizationBroker broker) {
        super(destination);
        this.broker = broker;
    }

    @Override
    public void addSubscription(ConnectionContext context, Subscription sub) throws Exception {
        // authorize subscription
        final SecurityContext securityContext = broker.checkSecurityContext(context);

        final AuthorizationMap authorizationMap = broker.getAuthorizationMap();
        // use the destination being filtered, instead of the destination from the consumerinfo in the subscription
        // since that could be a wildcard destination
        final ActiveMQDestination destination = next.getActiveMQDestination();

        Set<?> allowedACLs;
        if (!destination.isTemporary()) {
            allowedACLs = authorizationMap.getReadACLs(destination);
        } else {
            allowedACLs = authorizationMap.getTempDestinationReadACLs();
        }

        if (!securityContext.isBrokerContext() && allowedACLs != null && !securityContext.isInOneOf(allowedACLs) ) {
            throw new SecurityException("User " + securityContext.getUserName() + " is not authorized to read from: " + destination);
        }

        super.addSubscription(context, sub);
    }

}
