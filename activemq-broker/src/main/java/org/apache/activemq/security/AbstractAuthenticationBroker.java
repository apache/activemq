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

import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionInfo;

public abstract class AbstractAuthenticationBroker extends BrokerFilter implements AuthenticationBroker {

    protected final CopyOnWriteArrayList<SecurityContext> securityContexts =
        new CopyOnWriteArrayList<SecurityContext>();

    public AbstractAuthenticationBroker(Broker next) {
        super(next);
    }

    @Override
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        next.removeDestination(context, destination, timeout);

        for (SecurityContext sc : securityContexts) {
            sc.getAuthorizedWriteDests().remove(destination);
        }
    }

    @Override
    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        super.removeConnection(context, info, error);
        if (securityContexts.remove(context.getSecurityContext())) {
            context.setSecurityContext(null);
        }
    }

    public void refresh() {
        for (SecurityContext sc : securityContexts) {
            sc.getAuthorizedWriteDests().clear();
        }
    }
}
