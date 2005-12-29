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

import org.apache.activemq.Service;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.Topic;

/**
 * Abstraction to allow different recovery policies to be plugged
 * into the region implementations.  This is used by a topic to retroactively recover
 * messages that the subscription missed.
 * 
 * @version $Revision$
 */
public interface SubscriptionRecoveryPolicy extends Service {
    
    /**
     * A message was sent to the destination.
     * 
     * @param context
     * @param node
     * @return TODO
     * @throws Throwable
     */
    boolean add(ConnectionContext context, MessageReference message) throws Throwable;
    
    /**
     * Let a subscription recover message held by the policy.
     * 
     * @param context
     * @param topic TODO
     * @param topic 
     * @param node
     * @throws Throwable
     */
    void recover(ConnectionContext context, Topic topic, Subscription sub) throws Throwable;

}
