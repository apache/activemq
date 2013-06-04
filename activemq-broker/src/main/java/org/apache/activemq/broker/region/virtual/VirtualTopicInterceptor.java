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

import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.Message;
import org.apache.activemq.util.LRUCache;

/**
 * A Destination which implements <a href="http://activemq.org/site/virtual-destinations.html">Virtual Topic</a>
 */
public class VirtualTopicInterceptor extends DestinationFilter {

    private final String prefix;
    private final String postfix;
    private final boolean local;
    private final LRUCache<ActiveMQDestination, ActiveMQQueue> cache = new LRUCache<ActiveMQDestination, ActiveMQQueue>();

    public VirtualTopicInterceptor(Destination next, String prefix, String postfix, boolean local) {
        super(next);
        this.prefix = prefix;
        this.postfix = postfix;
        this.local = local;
    }

    public Topic getTopic() {
        return (Topic) this.next;
    }

    @Override
    public void send(ProducerBrokerExchange context, Message message) throws Exception {
        if (!message.isAdvisory() && !(local && message.getBrokerPath() != null)) {
            ActiveMQDestination queueConsumers = getQueueConsumersWildcard(message.getDestination());
            send(context, message, queueConsumers);
        }
        super.send(context, message);
    }

    protected ActiveMQDestination getQueueConsumersWildcard(ActiveMQDestination original) {
        ActiveMQQueue queue;
        synchronized (cache) {
            queue = cache.get(original);
            if (queue == null) {
                queue = new ActiveMQQueue(prefix + original.getPhysicalName() + postfix);
                cache.put(original, queue);
            }
        }
        return queue;
    }
}
