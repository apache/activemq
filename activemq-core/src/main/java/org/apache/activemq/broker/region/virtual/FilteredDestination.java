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

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.selector.SelectorParser;

/**
 * Represents a destination which is filtered using some predicate such as a selector
 * so that messages are only dispatched to the destination if they match the filter.
 *
 * @org.apache.xbean.XBean
 *
 * @version $Revision$
 */
public class FilteredDestination {
    
    private ActiveMQDestination destination;
    private String selector;
    private BooleanExpression filter;

    public boolean matches(MessageEvaluationContext context) throws JMSException {
        BooleanExpression booleanExpression = getFilter();
        if (booleanExpression == null) {
            return false;
        }
        return booleanExpression.matches(context);
    }

    public ActiveMQDestination getDestination() {
        return destination;
    }

    /**
     * The destination to send messages to if they match the filter
     */
    public void setDestination(ActiveMQDestination destination) {
        this.destination = destination;
    }

    public String getSelector() {
        return selector;
    }

    /**
     * Sets the JMS selector used to filter messages before forwarding them to this destination
     */
    public void setSelector(String selector) throws InvalidSelectorException {
        this.selector = selector;
        setFilter(new SelectorParser().parse(selector));
    }

    public BooleanExpression getFilter() {
        return filter;
    }

    public void setFilter(BooleanExpression filter) {
        this.filter = filter;
    }


    /**
     * Sets the destination property to the given queue name
     */
    public void setQueue(String queue) {
        setDestination(ActiveMQDestination.createDestination(queue, ActiveMQDestination.QUEUE_TYPE));
    }

    /**
     * Sets the destination property to the given topic name
     */
    public void setTopic(String topic) {
        setDestination(ActiveMQDestination.createDestination(topic, ActiveMQDestination.TOPIC_TYPE));
    }
}
