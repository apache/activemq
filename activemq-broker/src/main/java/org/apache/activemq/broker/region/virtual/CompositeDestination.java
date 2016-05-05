/*
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
import org.apache.activemq.command.CommandTypes;

public abstract class CompositeDestination implements VirtualDestination {

    private String name;
    private Collection forwardTo;
    private boolean forwardOnly = true;
    private boolean concurrentSend = false;

    @Override
    public Destination intercept(Destination destination) {
        return new CompositeDestinationFilter(destination, getForwardTo(), isForwardOnly(), isConcurrentSend());
    }

    @Override
    public void create(Broker broker, ConnectionContext context, ActiveMQDestination destination) {
    }

    @Override
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

    @Deprecated
    public boolean isCopyMessage() {
        return true;
    }

    /**
     * Sets whether a copy of the message will be sent to each destination.
     * Defaults to true so that the forward destination is set as the
     * destination of the message
     *
     * @deprecated this option will be removed in a later release, message are always copied.
     */
    @Deprecated
    public void setCopyMessage(boolean copyMessage) {
    }

    /**
     * when true, sends are done in parallel with the broker executor
     */
    public void setConcurrentSend(boolean concurrentSend) {
        this.concurrentSend = concurrentSend;
    }

    public boolean isConcurrentSend() {
        return this.concurrentSend;
    }

    @Override
    public ActiveMQDestination getMappedDestinations() {
        final ActiveMQDestination[] destinations = new ActiveMQDestination[forwardTo.size()];
        int i = 0;
        for (Object dest : forwardTo) {
            if (dest instanceof FilteredDestination) {
                FilteredDestination filteredDestination = (FilteredDestination) dest;
                destinations[i++] = filteredDestination.getDestination();
            } else if (dest instanceof ActiveMQDestination) {
                destinations[i++] = (ActiveMQDestination) dest;
            } else {
                // highly unlikely, but just in case!
                throw new IllegalArgumentException("Unknown mapped destination type " + dest);
            }
        }

        // used just for matching destination paths
        return new ActiveMQDestination(destinations) {
            @Override
            protected String getQualifiedPrefix() {
                return "mapped://";
            }

            @Override
            public byte getDestinationType() {
                return QUEUE_TYPE | TOPIC_TYPE;
            }

            @Override
            public byte getDataStructureType() {
                return CommandTypes.ACTIVEMQ_QUEUE | CommandTypes.ACTIVEMQ_TOPIC;
            }
        };
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (concurrentSend ? 1231 : 1237);
        result = prime * result + (forwardOnly ? 1231 : 1237);
        result = prime * result + ((forwardTo == null) ? 0 : forwardTo.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        CompositeDestination other = (CompositeDestination) obj;
        if (concurrentSend != other.concurrentSend) {
            return false;
        }

        if (forwardOnly != other.forwardOnly) {
            return false;
        }

        if (forwardTo == null) {
            if (other.forwardTo != null) {
                return false;
            }
        } else if (!forwardTo.equals(other.forwardTo)) {
            return false;
        }

        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name)) {
            return false;
        }

        return true;
    }
}
