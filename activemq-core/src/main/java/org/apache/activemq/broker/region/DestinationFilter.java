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
import java.util.Iterator;
import java.util.Set;
import java.util.List;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.Usage;

/**
 * 
 * @version $Revision$
 */
public class DestinationFilter implements Destination {

    private Destination next;

    public DestinationFilter(Destination next) {
        this.next = next;
    }

    public void acknowledge(ConnectionContext context, Subscription sub, MessageAck ack, MessageReference node)
        throws IOException {
        next.acknowledge(context, sub, ack, node);
    }

    public void addSubscription(ConnectionContext context, Subscription sub) throws Exception {
        next.addSubscription(context, sub);
    }

    public Message[] browse() {
        return next.browse();
    }

    public void dispose(ConnectionContext context) throws IOException {
        next.dispose(context);
    }

    public void gc() {
        next.gc();
    }

    public ActiveMQDestination getActiveMQDestination() {
        return next.getActiveMQDestination();
    }

    public DeadLetterStrategy getDeadLetterStrategy() {
        return next.getDeadLetterStrategy();
    }

    public DestinationStatistics getDestinationStatistics() {
        return next.getDestinationStatistics();
    }

    public String getName() {
        return next.getName();
    }

    public MemoryUsage getMemoryUsage() {
        return next.getMemoryUsage();
    }

    public void removeSubscription(ConnectionContext context, Subscription sub) throws Exception {
        next.removeSubscription(context, sub);
    }

    public void send(ProducerBrokerExchange context, Message messageSend) throws Exception {
        next.send(context, messageSend);
    }

    public void start() throws Exception {
        next.start();
    }

    public void stop() throws Exception {
        next.stop();
    }

    public List<Subscription> getConsumers() {
        return next.getConsumers();
    }

    /**
     * Sends a message to the given destination which may be a wildcard
     */
    protected void send(ProducerBrokerExchange context, Message message, ActiveMQDestination destination)
        throws Exception {
        Broker broker = context.getConnectionContext().getBroker();
        Set destinations = broker.getDestinations(destination);

        for (Iterator iter = destinations.iterator(); iter.hasNext();) {
            Destination dest = (Destination)iter.next();
            dest.send(context, message.copy());
        }
    }

    public MessageStore getMessageStore() {
        return next.getMessageStore();
    }

    public boolean isProducerFlowControl() {
        return next.isProducerFlowControl();
    }
    
    public void setProducerFlowControl(boolean value){
        next.setProducerFlowControl(value);
    }

    public void addProducer(ConnectionContext context, ProducerInfo info)
            throws Exception {
        next.addProducer(context, info);
        
    }

    public void removeProducer(ConnectionContext context, ProducerInfo info)
            throws Exception {
       next.removeProducer(context, info);
    }

    public int getMaxAuditDepth() {
       return next.getMaxAuditDepth();
    }

    public int getMaxProducersToAudit() {
        return next.getMaxProducersToAudit();
    }

    public boolean isEnableAudit() {
        return next.isEnableAudit();
    }
    
    public void setEnableAudit(boolean enableAudit) {
        next.setEnableAudit(enableAudit);
    }

    public void setMaxAuditDepth(int maxAuditDepth) {
       next.setMaxAuditDepth(maxAuditDepth);
    }

    
    public void setMaxProducersToAudit(int maxProducersToAudit) {
       next.setMaxProducersToAudit(maxProducersToAudit);
    }
    
    public boolean isActive() {
        return next.isActive();
    }

    public int getMaxPageSize() {
        return next.getMaxPageSize();
    }

    public void setMaxPageSize(int maxPageSize) {
        next.setMaxPageSize(maxPageSize);
    }

    public boolean isUseCache() {
        return next.isUseCache();
    }

    public void setUseCache(boolean useCache) {
        next.setUseCache(useCache);
    }   
    
    public int getMinimumMessageSize() {
        return next.getMinimumMessageSize();
    }

    public void setMinimumMessageSize(int minimumMessageSize) {
        next.setMinimumMessageSize(minimumMessageSize);
    }   
    
    public void wakeup() {
        next.wakeup();
    }

    public boolean isLazyDispatch() {
       return next.isLazyDispatch();
    }

    public void setLazyDispatch(boolean value) {
      next.setLazyDispatch(value);        
    }

    public void messageExpired(ConnectionContext context, PrefetchSubscription prefetchSubscription, MessageReference node) {
        next.messageExpired(context, prefetchSubscription, node);        
    }

	public boolean iterate() {
		return next.iterate();
	}

    public void fastProducer(ConnectionContext context,ProducerInfo producerInfo) {
       next.fastProducer(context, producerInfo);       
    }

   
    public void isFull(ConnectionContext context, Usage usage) {
       next.isFull(context, usage);
    }

   
    public void messageConsumed(ConnectionContext context,MessageReference messageReference) {
        next.messageConsumed(context, messageReference);
    }

    
    public void messageDelivered(ConnectionContext context,MessageReference messageReference) {
        next.messageDelivered(context, messageReference);
    }

    
    public void messageDiscarded(ConnectionContext context,MessageReference messageReference) {
        next.messageDiscarded(context, messageReference);
    }

    
    public void slowConsumer(ConnectionContext context, Subscription subs) {
       next.slowConsumer(context, subs);
    }

   
    public void messageExpired(ConnectionContext context, Subscription subs,MessageReference node) {
       next.messageExpired(context,subs, node);    
    }
}
