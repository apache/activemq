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
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.Message;

/**
 * A Destination which implements <a
 * href="http://activemq.org/site/virtual-destinations.html">Virtual Topic</a>
 * 
 * @version $Revision$
 */
public class VirtualTopicInterceptor extends DestinationFilter {

    private String prefix;
    private String postfix;

    public VirtualTopicInterceptor(Destination next, String prefix, String postfix) {
        super(next);
        this.prefix = prefix;
        this.postfix = postfix;
    }

    public void send(ProducerBrokerExchange context, Message message) throws Exception {
        ActiveMQDestination queueConsumers = getQueueConsumersWildcard(message.getDestination());
        send(context, message, queueConsumers);
    }

    protected ActiveMQDestination getQueueConsumersWildcard(ActiveMQDestination original) {
        return new ActiveMQQueue(prefix + original.getPhysicalName() + postfix);
    }
}
