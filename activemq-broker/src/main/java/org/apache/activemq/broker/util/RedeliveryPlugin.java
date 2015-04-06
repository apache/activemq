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
package org.apache.activemq.broker.util;

import java.io.IOException;

import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.policy.RedeliveryPolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.filter.AnyDestination;
import org.apache.activemq.state.ProducerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replace regular DLQ handling with redelivery via a resend to the original destination
 * after a delay
 * A destination matching RedeliveryPolicy controls the quantity and delay for re-sends
 * If there is no matching policy or an existing policy limit is exceeded by default
 * regular DLQ processing resumes. This is controlled via sendToDlqIfMaxRetriesExceeded
 * and fallbackToDeadLetter
 *
 * @org.apache.xbean.XBean element="redeliveryPlugin"
 */
public class RedeliveryPlugin extends BrokerPluginSupport {
    private static final Logger LOG = LoggerFactory.getLogger(RedeliveryPlugin.class);
    public static final String REDELIVERY_DELAY = "redeliveryDelay";

    RedeliveryPolicyMap redeliveryPolicyMap = new RedeliveryPolicyMap();
    boolean sendToDlqIfMaxRetriesExceeded = true;
    private boolean fallbackToDeadLetter = true;

    @Override
    public Broker installPlugin(Broker broker) throws Exception {
        if (!broker.getBrokerService().isSchedulerSupport()) {
            throw new IllegalStateException("RedeliveryPlugin requires schedulerSupport=true on the broker");
        }
        validatePolicyDelay(1000);
        return super.installPlugin(broker);
    }

    /*
     * sending to dlq is called as part of a poison ack processing, before the message is acknowledged  and removed
     * by the destination so a delay is vital to avoid resending before it has been consumed
     */
    private void validatePolicyDelay(long limit) {
        final ActiveMQDestination matchAll = new AnyDestination(new ActiveMQDestination[]{new ActiveMQQueue(">"), new ActiveMQTopic(">")});
        for (Object entry : redeliveryPolicyMap.get(matchAll)) {
            RedeliveryPolicy redeliveryPolicy = (RedeliveryPolicy) entry;
            validateLimit(limit, redeliveryPolicy);
        }
        RedeliveryPolicy defaultEntry = redeliveryPolicyMap.getDefaultEntry();
        if (defaultEntry != null) {
            validateLimit(limit, defaultEntry);
        }
    }

    private void validateLimit(long limit, RedeliveryPolicy redeliveryPolicy) {
        if (redeliveryPolicy.getInitialRedeliveryDelay() < limit) {
            throw new IllegalStateException("RedeliveryPolicy initialRedeliveryDelay must exceed: " + limit + ". " + redeliveryPolicy);
        }
        if (redeliveryPolicy.getRedeliveryDelay() < limit) {
            throw new IllegalStateException("RedeliveryPolicy redeliveryDelay must exceed: " + limit + ". " + redeliveryPolicy);
        }
    }

    public RedeliveryPolicyMap getRedeliveryPolicyMap() {
        return redeliveryPolicyMap;
    }

    public void setRedeliveryPolicyMap(RedeliveryPolicyMap redeliveryPolicyMap) {
        this.redeliveryPolicyMap = redeliveryPolicyMap;
    }

    public boolean isSendToDlqIfMaxRetriesExceeded() {
        return sendToDlqIfMaxRetriesExceeded;
    }

    /**
     * What to do if the maxretries on a matching redelivery policy is exceeded.
     * when true, the region broker DLQ processing will be used via sendToDeadLetterQueue
     * when false, there is no action
     * @param sendToDlqIfMaxRetriesExceeded
     */
    public void setSendToDlqIfMaxRetriesExceeded(boolean sendToDlqIfMaxRetriesExceeded) {
        this.sendToDlqIfMaxRetriesExceeded = sendToDlqIfMaxRetriesExceeded;
    }

    public boolean isFallbackToDeadLetter() {
        return fallbackToDeadLetter;
    }

    /**
     * What to do if there is no matching redelivery policy for a destination.
     * when true, the region broker DLQ processing will be used via sendToDeadLetterQueue
     * when false, there is no action
     * @param fallbackToDeadLetter
     */
    public void setFallbackToDeadLetter(boolean fallbackToDeadLetter) {
        this.fallbackToDeadLetter = fallbackToDeadLetter;
    }

    @Override
    public boolean sendToDeadLetterQueue(ConnectionContext context, MessageReference messageReference, Subscription subscription, Throwable poisonCause) {
        if (messageReference.isExpired()) {
            // there are two uses of  sendToDeadLetterQueue, we are only interested in valid messages
            return super.sendToDeadLetterQueue(context, messageReference, subscription, poisonCause);
        } else {
            try {
                Destination regionDestination = (Destination) messageReference.getRegionDestination();
                final RedeliveryPolicy redeliveryPolicy = redeliveryPolicyMap.getEntryFor(regionDestination.getActiveMQDestination());
                if (redeliveryPolicy != null) {
                    final int maximumRedeliveries = redeliveryPolicy.getMaximumRedeliveries();
                    int redeliveryCount = messageReference.getRedeliveryCounter();
                    if (RedeliveryPolicy.NO_MAXIMUM_REDELIVERIES == maximumRedeliveries || redeliveryCount < maximumRedeliveries) {

                        long delay = redeliveryPolicy.getInitialRedeliveryDelay();
                        for (int i = 0; i < redeliveryCount; i++) {
                            delay = redeliveryPolicy.getNextRedeliveryDelay(delay);
                        }

                        scheduleRedelivery(context, messageReference, delay, ++redeliveryCount);
                    } else if (isSendToDlqIfMaxRetriesExceeded()) {
                        return super.sendToDeadLetterQueue(context, messageReference, subscription, poisonCause);
                    } else {
                        LOG.debug("Discarding message that exceeds max redelivery count({}), {}", maximumRedeliveries, messageReference.getMessageId());
                    }
                } else if (isFallbackToDeadLetter()) {
                    return super.sendToDeadLetterQueue(context, messageReference, subscription, poisonCause);
                } else {
                    LOG.debug("Ignoring dlq request for: {}, RedeliveryPolicy not found (and no fallback) for: {}", messageReference.getMessageId(), regionDestination.getActiveMQDestination());
                }

                return false;
            } catch (Exception exception) {
                // abort the ack, will be effective if client use transactions or individual ack with sync send
                RuntimeException toThrow =  new RuntimeException("Failed to schedule redelivery for: " + messageReference.getMessageId(), exception);
                LOG.error(toThrow.toString(), exception);
                throw toThrow;
            }
        }
    }

    private void scheduleRedelivery(ConnectionContext context, MessageReference messageReference, long delay, int redeliveryCount) throws Exception {
        if (LOG.isTraceEnabled()) {
            Destination regionDestination = (Destination) messageReference.getRegionDestination();
            LOG.trace("redelivery #{} of: {} with delay: {}, dest: {}", new Object[]{
                    redeliveryCount, messageReference.getMessageId(), delay, regionDestination.getActiveMQDestination()
            });
        }
        final Message old = messageReference.getMessage();
        Message message = old.copy();

        message.setTransactionId(null);
        message.setMemoryUsage(null);
        message.removeProperty(ScheduledMessage.AMQ_SCHEDULED_ID);

        message.setProperty(REDELIVERY_DELAY, delay);
        message.setProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, delay);
        message.setRedeliveryCounter(redeliveryCount);

        boolean originalFlowControl = context.isProducerFlowControl();
        try {
            context.setProducerFlowControl(false);
            ProducerInfo info = new ProducerInfo();
            ProducerState state = new ProducerState(info);
            ProducerBrokerExchange producerExchange = new ProducerBrokerExchange();
            producerExchange.setProducerState(state);
            producerExchange.setMutable(true);
            producerExchange.setConnectionContext(context);
            context.getBroker().send(producerExchange, message);
        } finally {
            context.setProducerFlowControl(originalFlowControl);
        }
    }

}
