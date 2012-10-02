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
package org.apache.activemq.broker.region.virtual;

import java.util.Collection;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;

/**
 * 
 * 
 */
public abstract class CompositeDestination implements VirtualDestination {

    private String name;
    private Collection forwardTo;
    private boolean forwardOnly = true;
    private boolean copyMessage = true;

    public Destination intercept(Destination destination) {
        return new CompositeDestinationFilter(destination, getForwardTo(), isForwardOnly(), isCopyMessage());
    }
    
    public void create(Broker broker, ConnectionContext context, ActiveMQDestination destination) {
    }

    public void remove(Destination destination) {        
    }

    public String getName() {
        return name;
    }

    /**
     * Sets the name of this composite destination
     */
    public void setName(String name) {
        this.name = name;
    }

    public Collection getForwardTo() {
        return forwardTo;
    }

    /**
     * Sets the list of destinations to forward to
     */
    public void setForwardTo(Collection forwardDestinations) {
        this.forwardTo = forwardDestinations;
    }

    public boolean isForwardOnly() {
        return forwardOnly;
    }

    /**
     * Sets if the virtual destination is forward only (and so there is no
     * physical queue to match the virtual queue) or if there is also a physical
     * queue with the same name).
     */
    public void setForwardOnly(boolean forwardOnly) {
        this.forwardOnly = forwardOnly;
    }

    public boolean isCopyMessage() {
        return copyMessage;
    }

    /**
     * Sets whether a copy of the message will be sent to each destination.
     * Defaults to true so that the forward destination is set as the
     * destination of the message
     */
    public void setCopyMessage(boolean copyMessage) {
        this.copyMessage = copyMessage;
    }

}
