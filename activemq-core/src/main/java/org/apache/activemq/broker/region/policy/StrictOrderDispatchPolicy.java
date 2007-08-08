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

import java.util.Iterator;
import java.util.List;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.filter.MessageEvaluationContext;

/**
 * Dispatch policy that causes every subscription to see messages in the same
 * order.
 * 
 * @org.apache.xbean.XBean
 * @version $Revision$
 */
public class StrictOrderDispatchPolicy implements DispatchPolicy {

    /**
     * @param node
     * @param msgContext
     * @param consumers
     * @return true if dispatched
     * @throws Exception
     * @see org.apache.activemq.broker.region.policy.DispatchPolicy#dispatch(org.apache.activemq.broker.region.MessageReference,
     *      org.apache.activemq.filter.MessageEvaluationContext, java.util.List)
     */
    public boolean dispatch(MessageReference node, MessageEvaluationContext msgContext, List consumers) throws Exception {
        // Big synch here so that only 1 message gets dispatched at a time.
        // Ensures
        // Everyone sees the same order.
        synchronized (consumers) {
            int count = 0;
            for (Iterator iter = consumers.iterator(); iter.hasNext();) {
                Subscription sub = (Subscription)iter.next();

                // Only dispatch to interested subscriptions
                if (!sub.matches(node, msgContext))
                    continue;

                sub.add(node);
                count++;
            }
            return count > 0;
        }
    }

}
