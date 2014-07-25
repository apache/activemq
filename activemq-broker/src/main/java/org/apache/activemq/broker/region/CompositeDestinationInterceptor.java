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
package org.apache.activemq.broker.region;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;

/**
 * Represents a Composite Pattern of a {@link DestinationInterceptor}
 * 
 * 
 */
public class CompositeDestinationInterceptor implements DestinationInterceptor {

    private volatile DestinationInterceptor[] interceptors;

    public CompositeDestinationInterceptor(final DestinationInterceptor[] interceptors) {
        this.interceptors = interceptors;
    }

    public Destination intercept(Destination destination) {
        for (int i = 0; i < interceptors.length; i++) {
            destination = interceptors[i].intercept(destination);
        }
        return destination;
    }

   
    public void remove(Destination destination) {
        for (int i = 0; i < interceptors.length; i++) {
            interceptors[i].remove(destination);
        } 
    }

    public void create(Broker broker, ConnectionContext context, ActiveMQDestination destination) throws Exception {
        for (int i = 0; i < interceptors.length; i++) {
            interceptors[i].create(broker, context, destination);
        }
    }

    public void setInterceptors(final DestinationInterceptor[] interceptors) {
        this.interceptors = interceptors;
    }

    public DestinationInterceptor[] getInterceptors() {
        return interceptors;
    }

}
