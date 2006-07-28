/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.region.policy;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.ActiveMQMessageTransformation;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.util.IdGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.Message;
import javax.jms.MessageListener;

/**
 * This implementation of {@link SubscriptionRecoveryPolicy} will perform a user
 * specific query mechanism to load any messages they may have missed.
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision$
 */
public class QueryBasedSubscriptionRecoveryPolicy implements SubscriptionRecoveryPolicy {

    private static final Log log = LogFactory.getLog(QueryBasedSubscriptionRecoveryPolicy.class);

    private MessageQuery query;
    private AtomicLong messageSequence = new AtomicLong(0);
    private IdGenerator idGenerator = new IdGenerator();
    private ProducerId producerId = createProducerId();

    public SubscriptionRecoveryPolicy copy() {
        QueryBasedSubscriptionRecoveryPolicy rc = new QueryBasedSubscriptionRecoveryPolicy();
        rc.setQuery(query);
        return rc;
    }

    public boolean add(ConnectionContext context, MessageReference message) throws Exception {
        return query.validateUpdate(message.getMessage());
    }

    public void recover(ConnectionContext context, final Topic topic, final Subscription sub) throws Exception {
        if (query != null) {
            final MessageEvaluationContext msgContext = context.getMessageEvaluationContext();
            try {
                ActiveMQDestination destination = sub.getConsumerInfo().getDestination();
                query.execute(destination, new MessageListener() {
                    public void onMessage(Message message) {
                        dispatchInitialMessage(message, topic, msgContext, sub);
                    }
                });
            }
            finally {
                msgContext.clear();
            }
        }
    }

    public void start() throws Exception {
        if (query == null) {
            throw new IllegalArgumentException("No query property configured");
        }
    }

    public void stop() throws Exception {
    }

    public MessageQuery getQuery() {
        return query;
    }

    /**
     * Sets the query strategy to load initial messages
     */
    public void setQuery(MessageQuery query) {
        this.query = query;
    }
    
    public org.apache.activemq.command.Message[] browse(ActiveMQDestination dest) throws Exception{
        return new org.apache.activemq.command.Message[0];
    }

    protected void dispatchInitialMessage(Message message,  Destination regionDestination, MessageEvaluationContext msgContext, Subscription sub) {
        try {
            ActiveMQMessage activeMessage = ActiveMQMessageTransformation.transformMessage(message, null);
            ActiveMQDestination destination = activeMessage.getDestination();
            if (destination == null) {
                destination = sub.getConsumerInfo().getDestination();
                activeMessage.setDestination(destination);
            }
            activeMessage.setRegionDestination(regionDestination);
            configure(activeMessage);
            msgContext.setDestination(destination);
            msgContext.setMessageReference(activeMessage);
            if (sub.matches(activeMessage, msgContext)) {
                sub.add(activeMessage);
            }
        }
        catch (Throwable e) {
            log.warn("Failed to dispatch initial message: " + message + " into subscription. Reason: " + e, e);
        }
    }

    protected void configure(ActiveMQMessage msg) {
        long sequenceNumber = messageSequence.incrementAndGet();
        msg.setMessageId(new MessageId(producerId, sequenceNumber));
        msg.onSend();
        msg.setProducerId(producerId);
    }

    protected ProducerId createProducerId() {
        String id = idGenerator.generateId();
        ConnectionId connectionId = new ConnectionId(id);
        SessionId sessionId = new SessionId(connectionId, 1);
        return new ProducerId(sessionId, 1);
    }
}
