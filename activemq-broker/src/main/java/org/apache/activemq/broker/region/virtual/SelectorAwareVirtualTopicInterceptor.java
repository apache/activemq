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

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.Message;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.filter.NonCachedMessageEvaluationContext;
import org.apache.activemq.plugin.SubQueueSelectorCacheBroker;
import org.apache.activemq.selector.SelectorParser;
import org.apache.activemq.util.LRUCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class SelectorAwareVirtualTopicInterceptor extends VirtualTopicInterceptor {
    private static final Logger LOG = LoggerFactory.getLogger(SelectorAwareVirtualTopicInterceptor.class);
    LRUCache<String,BooleanExpression> expressionCache = new LRUCache<String,BooleanExpression>();
    private final SubQueueSelectorCacheBroker selectorCachePlugin;

    public SelectorAwareVirtualTopicInterceptor(Destination next, VirtualTopic virtualTopic) {
        super(next, virtualTopic);
        selectorCachePlugin = (SubQueueSelectorCacheBroker)
                ((Topic)next).createConnectionContext().getBroker().getAdaptor(SubQueueSelectorCacheBroker.class);
    }

    /**
     * Respect the selectors of the subscriptions to ensure only matched messages are dispatched to
     * the virtual queues, hence there is no build up of unmatched messages on these destinations
     */
    @Override
    protected boolean shouldDispatch(final Broker broker, Message message, Destination dest) throws IOException {
        //first validate that the prefix matches in the super class
        if (super.shouldDispatch(broker, message, dest)) {
            boolean matches = false;
            MessageEvaluationContext msgContext = new NonCachedMessageEvaluationContext();
            msgContext.setDestination(dest.getActiveMQDestination());
            msgContext.setMessageReference(message);
            List<Subscription> subs = dest.getConsumers();
            for (Subscription sub : subs) {
                if (sub.matches(message, msgContext)) {
                    matches = true;
                    break;
                }
            }
            if (matches == false) {
                matches = tryMatchingCachedSubs(broker, dest, msgContext);
            }
            return matches;
        }
        return false;
    }

    private boolean tryMatchingCachedSubs(final Broker broker, Destination dest, MessageEvaluationContext msgContext) {
        boolean matches = false;
        LOG.debug("No active consumer match found. Will try cache if configured...");

        if (selectorCachePlugin != null) {
            final Set<String> selectors = selectorCachePlugin.getSelector(dest.getActiveMQDestination().getQualifiedName());
            if (selectors != null) {
                for (String selector : selectors) {
                    try {
                        final BooleanExpression expression = getExpression(selector);
                        matches = expression.matches(msgContext);
                        if (matches) {
                            return true;
                        }
                    } catch (Exception e) {
                        LOG.error(e.getMessage(), e);
                    }
                }
            }
        }
        return matches;
    }

    private BooleanExpression getExpression(String selector) throws Exception{
        BooleanExpression result;
        synchronized(expressionCache){
            result = expressionCache.get(selector);
            if (result == null){
                result = compileSelector(selector);
                expressionCache.put(selector,result);
            }
        }
        return result;
    }

    /**
     * Pre-compile the JMS selector.
     *
     * @param selectorExpression The non-null JMS selector expression.
     */
    private BooleanExpression compileSelector(final String selectorExpression) throws Exception {
        return SelectorParser.parse(selectorExpression);
    }
}
