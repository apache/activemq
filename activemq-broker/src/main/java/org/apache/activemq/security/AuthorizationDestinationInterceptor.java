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

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.command.ActiveMQDestination;

/**
 * Adds AuthorizationDestinationFilter on intercept()
 */
public class AuthorizationDestinationInterceptor implements DestinationInterceptor {

    private final AuthorizationBroker broker;

    public AuthorizationDestinationInterceptor(AuthorizationBroker broker) {
        this.broker = broker;
    }

    @Override
    public Destination intercept(Destination destination) {
        return new AuthorizationDestinationFilter(destination, broker);
    }

    @Override
    public void remove(Destination destination) {
        // do nothing
    }

    @Override
    public void create(Broker broker, ConnectionContext context, ActiveMQDestination destination) throws Exception {
        // do nothing
    }

}
