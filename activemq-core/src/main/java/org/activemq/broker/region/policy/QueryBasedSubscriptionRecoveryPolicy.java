/**
 * <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
 *
 * Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/
package org.activemq.broker.region.policy;

import org.activemq.ActiveMQMessageTransformation;
import org.activemq.broker.ConnectionContext;
import org.activemq.broker.region.MessageReference;
import org.activemq.broker.region.Subscription;
import org.activemq.command.ActiveMQDestination;
import org.activemq.command.ActiveMQMessage;
import org.activemq.filter.MessageEvaluationContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.Message;
import javax.jms.MessageListener;

/**
 * This implementation of {@link SubscriptionRecoveryPolicy} will perform a user
 * specific query mechanism to load any messages they may have missed.
 * 
 * @org.xbean.XBean
 * 
 * @version $Revision$
 */
public class QueryBasedSubscriptionRecoveryPolicy implements SubscriptionRecoveryPolicy {

    private static final Log log = LogFactory.getLog(QueryBasedSubscriptionRecoveryPolicy.class);

    private MessageQuery query;

    public void add(ConnectionContext context, MessageReference message) throws Throwable {
    }

    public void recover(ConnectionContext context, final Subscription sub) throws Throwable {
        if (query != null) {
            final MessageEvaluationContext msgContext = context.getMessageEvaluationContext();
            try {
                ActiveMQDestination destination = sub.getConsumerInfo().getDestination();
                query.execute(destination, new MessageListener() {
                    public void onMessage(Message message) {
                        dispatchInitialMessage(message, msgContext, sub);
                    }
                });
            }
            finally {
                msgContext.clear();
            }
        }
    }

    public void start() throws Exception {
        if (query != null) {
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

    protected void dispatchInitialMessage(Message message, MessageEvaluationContext msgContext, Subscription sub) {
        try {
            ActiveMQMessage activeMessage = ActiveMQMessageTransformation.transformMessage(message, null);
            msgContext.setDestination(activeMessage.getDestination());
            msgContext.setMessageReference(activeMessage);
            if (sub.matches(activeMessage, msgContext)) {
                sub.add(activeMessage);
            }
        }
        catch (Throwable e) {
            log.warn("Failed to dispatch initial message: " + message + " into subscription. Reason: " + e, e);
        }
    }
}
