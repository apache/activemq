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
import java.util.Iterator;
import java.util.List;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * dispatch policy that ignores lower priority duplicate network consumers,
 * used in conjunction with network bridge suppresDuplicateTopicSubscriptions
 * 
 * @org.apache.xbean.XBean
 */
public class PriorityNetworkDispatchPolicy extends SimpleDispatchPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(PriorityNetworkDispatchPolicy.class);
    @Override
    public boolean dispatch(MessageReference node,
            MessageEvaluationContext msgContext,
            List<Subscription> consumers) throws Exception {
        
        List<Subscription> duplicateFreeSubs = new ArrayList<Subscription>();
        synchronized (consumers) {
            for (Subscription sub: consumers) {
                ConsumerInfo info = sub.getConsumerInfo();
                if (info.isNetworkSubscription()) {    
                    boolean highestPrioritySub = true;
                    for (Iterator<Subscription> it =  duplicateFreeSubs.iterator(); it.hasNext(); ) {
                        Subscription candidate = it.next();
                        if (matches(candidate, info)) {
                            if (hasLowerPriority(candidate, info)) {
                                it.remove();
                            } else {
                                // higher priority matching sub exists
                                highestPrioritySub = false;
                                LOG.debug("ignoring lower priority: {} [{}, {}] in favour of: {} [{}, {}]",
                                        new Object[]{ candidate,
                                                candidate.getConsumerInfo().getNetworkConsumerIds(),
                                                candidate.getConsumerInfo().getNetworkConsumerIds(),
                                                sub,
                                                sub.getConsumerInfo().getNetworkConsumerIds(),
                                                sub.getConsumerInfo().getNetworkConsumerIds() });
                            }
                        }
                    }
                    if (highestPrioritySub) {
                        duplicateFreeSubs.add(sub);
                    } 
                } else {
                    duplicateFreeSubs.add(sub);
                }
            }
        }
        
        return super.dispatch(node, msgContext, duplicateFreeSubs);
    }

    private boolean hasLowerPriority(Subscription candidate,
            ConsumerInfo info) {
       return candidate.getConsumerInfo().getPriority() < info.getPriority();
    }

    private boolean matches(Subscription candidate, ConsumerInfo info) {
        boolean matched = false;
        for (ConsumerId candidateId: candidate.getConsumerInfo().getNetworkConsumerIds()) {
            for (ConsumerId subId: info.getNetworkConsumerIds()) {
                if (candidateId.equals(subId)) {
                    matched = true;
                    break;
                }
            }
        }
        return matched;
    }

}
