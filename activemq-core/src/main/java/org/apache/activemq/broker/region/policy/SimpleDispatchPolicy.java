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

import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArrayList;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.filter.MessageEvaluationContext;

import java.util.Iterator;

/**
 * Simple dispatch policy that sends a message to every subscription that 
 * matches the message.
 * 
 * @org.xbean.XBean
 * 
 * @version $Revision$
 */
public class SimpleDispatchPolicy implements DispatchPolicy {

    public boolean dispatch(ConnectionContext context, MessageReference node, MessageEvaluationContext msgContext, CopyOnWriteArrayList consumers) throws Throwable {
        int count = 0;
        for (Iterator iter = consumers.iterator(); iter.hasNext();) {
            Subscription sub = (Subscription) iter.next();
            
            // Don't deliver to browsers
            if( sub.getConsumerInfo().isBrowser() )
                continue;
            // Only dispatch to interested subscriptions
            if (!sub.matches(node, msgContext)) 
                continue;
            
            sub.add(node);
            count++;
        }
        return count > 0;
    }

}
