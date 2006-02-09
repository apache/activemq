/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
 */
package org.apache.activemq.broker.region.policy;


import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.filter.MessageEvaluationContext;

import java.util.Iterator;
import java.util.List;

/**
 * Simple dispatch policy that sends a message to every subscription that 
 * matches the message.
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision$
 */
public class RoundRobinDispatchPolicy implements DispatchPolicy {

    public boolean dispatch(ConnectionContext newParam, MessageReference node, MessageEvaluationContext msgContext, List consumers) throws Throwable {
        
        // Big synch here so that only 1 message gets dispatched at a time.  Ensures 
        // Everyone sees the same order and that the consumer list is not used while
        // it's being rotated.
        synchronized(consumers) {
            int count = 0;
            
            for (Iterator iter = consumers.iterator(); iter.hasNext();) {
                Subscription sub = (Subscription) iter.next();
                
                // Only dispatch to interested subscriptions
                if (!sub.matches(node, msgContext)) 
                    continue;
                
                sub.add(node);
                count++;
            }
            
            // Rotate the consumer list.
            try {
                consumers.add(consumers.remove(0));
            } catch (Throwable bestEffort) {
            }
            return count > 0;
        }        
    }


}
