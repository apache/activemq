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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.filter.DestinationFilter;

/**
 * Creates <a href="https://activemq.apache.org/virtual-destinations">Virtual
 * Topics</a> using a prefix and postfix. The virtual destination creates a
 * wildcard that is then used to look up all active queue subscriptions which
 * match.
 *
 * @org.apache.xbean.XBean
 */
public class VirtualTopic implements VirtualDestination {

    private String prefix = "Consumer.*.";
    private String postfix = "";
    private String name = ">";
    private boolean selectorAware = false;
    private boolean local = false;
    private boolean concurrentSend = false;
    private boolean transactedSend = false;
    private boolean dropOnResourceLimit = false;
    private boolean setOriginalDestination = true;

    @Override
    public ActiveMQDestination getVirtualDestination() {
        return new ActiveMQTopic(getName());
    }

    @Override
    public Destination intercept(Destination destination) {
        return selectorAware ? new SelectorAwareVirtualTopicInterceptor(destination, this) :
                new VirtualTopicInterceptor(destination, this);
    }

    @Override
    public ActiveMQDestination getMappedDestinations() {
        return new ActiveMQQueue(prefix + name + postfix);
    }

    @Override
    public Destination interceptMappedDestination(Destination destination) {
        // do a reverse map from destination to get actual virtual destination
        final String physicalName = destination.getActiveMQDestination().getPhysicalName();
        final Pattern pattern = Pattern.compile(getRegex(prefix) + "(.*)" + getRegex(postfix));
        final Matcher matcher = pattern.matcher(physicalName);
        if (matcher.matches()) {
            final String virtualName = matcher.group(1);
            return new MappedQueueFilter(new ActiveMQTopic(virtualName), destination);
        }
        return destination;
    }

    private String getRegex(String part) {
        StringBuilder builder = new StringBuilder();
        for (char c : part.toCharArray()) {
            switch (c) {
                case '.':
                    builder.append("\\.");
                    break;
                case '*':
                    builder.append("[^\\.]*");
                    break;
                default:
                    builder.append(c);
            }
        }
        return builder.toString();
    }

    @Override
    public void create(Broker broker, ConnectionContext context, ActiveMQDestination destination) throws Exception {
        if (destination.isQueue() && destination.isPattern()) {
            DestinationFilter filter = DestinationFilter.parseFilter(new ActiveMQQueue(prefix + DestinationFilter.ANY_DESCENDENT));
            if (filter.matches(destination)) {
                broker.addDestination(context, destination, false);

            }
        }
    }

    @Override
    public void remove(Destination destination) {
    }

    // Properties
    // -------------------------------------------------------------------------

    public String getPostfix() {
        return postfix;
    }

    /**
     * Sets any postix used to identify the queue consumers
     */
    public void setPostfix(String postfix) {
        this.postfix = postfix;
    }

    public String getPrefix() {
        return prefix;
    }

    /**
     * Sets the prefix wildcard used to identify the queue consumers for a given
     * topic
     */
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * Indicates whether the selectors of consumers are used to determine
     * dispatch to a virtual destination, when true only messages matching an
     * existing consumer will be dispatched.
     *
     * @param selectorAware
     *            when true take consumer selectors into consideration
     */
    public void setSelectorAware(boolean selectorAware) {
        this.selectorAware = selectorAware;
    }

    public boolean isSelectorAware() {
        return selectorAware;
    }

    public boolean isLocal() {
        return local;
    }

    public void setLocal(boolean local) {
        this.local = local;
    }

    @Override
    public String toString() {
        return new StringBuilder("VirtualTopic:").append(prefix).append(',').append(name).append(',').
                                                  append(postfix).append(',').append(selectorAware).
                                                  append(',').append(local).toString();
    }

    public boolean isConcurrentSend() {
        return concurrentSend;
    }

    /**
     * When true, dispatch to matching destinations in parallel (in multiple threads)
     * @param concurrentSend
     */
    public void setConcurrentSend(boolean concurrentSend) {
        this.concurrentSend = concurrentSend;
    }

    public boolean isTransactedSend() {
        return transactedSend;
    }

    /**
     * When true, dispatch to matching destinations always uses a transaction.
     * @param transactedSend
     */
    public void setTransactedSend(boolean transactedSend) {
        this.transactedSend = transactedSend;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (concurrentSend ? 1231 : 1237);
        result = prime * result + (local ? 1231 : 1237);
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((postfix == null) ? 0 : postfix.hashCode());
        result = prime * result + ((prefix == null) ? 0 : prefix.hashCode());
        result = prime * result + (selectorAware ? 1231 : 1237);
        result = prime * result + (transactedSend ? 1231 : 1237);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        VirtualTopic other = (VirtualTopic) obj;
        if (concurrentSend != other.concurrentSend)
            return false;
        if (local != other.local)
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (postfix == null) {
            if (other.postfix != null)
                return false;
        } else if (!postfix.equals(other.postfix))
            return false;
        if (prefix == null) {
            if (other.prefix != null)
                return false;
        } else if (!prefix.equals(other.prefix))
            return false;
        if (selectorAware != other.selectorAware)
            return false;
        if (transactedSend != other.transactedSend)
            return false;
        return true;
    }

    public boolean isDropOnResourceLimit() {
        return dropOnResourceLimit;
    }

    public void setDropOnResourceLimit(boolean dropOnResourceLimit) {
        this.dropOnResourceLimit = dropOnResourceLimit;
    }

    public boolean isSetOriginalDestination() {
        return setOriginalDestination;
    }

    public void setSetOriginalDestination(boolean setOriginalDestination) {
        this.setOriginalDestination = setOriginalDestination;
    }
}
