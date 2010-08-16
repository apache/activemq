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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Simple dispatch policy that sends a message to every subscription that
 * matches the message.
 * 
 * @org.apache.xbean.XBean
 * @version $Revision$
 */
public class RoundRobinDispatchPolicy implements DispatchPolicy {
    static final Log LOG = LogFactory.getLog(RoundRobinDispatchPolicy.class);

    /**
     * @param node
     * @param msgContext
     * @param consumers
     * @return true if dispatched
     * @throws Exception
     * @see org.apache.activemq.broker.region.policy.DispatchPolicy#dispatch(org.apache.activemq.broker.region.MessageReference,
     *      org.apache.activemq.filter.MessageEvaluationContext, java.util.List)
     */
    public boolean dispatch(MessageReference node,
            MessageEvaluationContext msgContext, List<Subscription> consumers)
            throws Exception {
        int count = 0;

        Subscription firstMatchingConsumer = null;
        synchronized (consumers) {
            for (Iterator<Subscription> iter = consumers.iterator(); iter
                    .hasNext();) {
                Subscription sub = iter.next();

                // Only dispatch to interested subscriptions
                if (!sub.matches(node, msgContext)) {
                    sub.unmatched(node);
                    continue;
                }

                if (firstMatchingConsumer == null) {
                    firstMatchingConsumer = sub;
                }

                sub.add(node);
                count++;
            }

            if (firstMatchingConsumer != null) {
                // Rotate the consumer list.
                try {
                    consumers.remove(firstMatchingConsumer);
                    consumers.add(firstMatchingConsumer);
                } catch (Throwable bestEffort) {
                }
            }
        }
        return count > 0;
    }
}
