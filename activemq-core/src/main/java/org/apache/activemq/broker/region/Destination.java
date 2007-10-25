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

import java.io.IOException;

import org.apache.activemq.Service;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.SystemUsage;

/**
 * @version $Revision: 1.12 $
 */
public interface Destination extends Service {

    void addSubscription(ConnectionContext context, Subscription sub) throws Exception;

    void removeSubscription(ConnectionContext context, Subscription sub) throws Exception;

    void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception;

    boolean lock(MessageReference node, LockOwner lockOwner);

    void acknowledge(ConnectionContext context, Subscription sub, final MessageAck ack, final MessageReference node) throws IOException;

    void gc();

    ActiveMQDestination getActiveMQDestination();

    MemoryUsage getBrokerMemoryUsage();

    void dispose(ConnectionContext context) throws IOException;

    DestinationStatistics getDestinationStatistics();

    DeadLetterStrategy getDeadLetterStrategy();

    Message[] browse();

    String getName();

    MessageStore getMessageStore();
    
    boolean isProducerFlowControl();
    
    void setProducerFlowControl(boolean value);
}
