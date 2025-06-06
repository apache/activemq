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
package org.apache.activemq.broker.region.policy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.StringJoiner;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.filter.XPathExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Priority dispatch policy that sends a message to every subscription that
 * matches the message in consumer priority order.
 * 
 * @org.apache.xbean.XBean
 * 
 */
public class PriorityDispatchPolicy extends SimpleDispatchPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(PriorityDispatchPolicy.class);

    private final Comparator<? super Subscription> orderedCompare = new Comparator<Subscription>() {
        @Override
        public int compare(Subscription o1, Subscription o2) {
            // We want the list sorted in descending order
            return o2.getConsumerInfo().getPriority() - o1.getConsumerInfo().getPriority();
        }
    };

    public boolean dispatch(MessageReference node, MessageEvaluationContext msgContext, List<Subscription> consumers)
            throws Exception {
        ArrayList<Subscription> ordered = new ArrayList<Subscription>(consumers);
        Collections.sort(ordered, orderedCompare);

        if (LOG.isDebugEnabled() && ordered.size() > 0) {
            StringJoiner stringJoiner = new StringJoiner(",");
            for (Subscription sub : ordered) {
                stringJoiner.add(String.valueOf(sub.getConsumerInfo().getPriority()));
            }
            LOG.debug("Ordered priorities: {}", stringJoiner);
        }
        return super.dispatch(node, msgContext, ordered);
    }

}
