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

import java.util.List;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.filter.MessageEvaluationContext;

/**
 * Abstraction to allow different dispatching policies to be plugged
 * into the  topic region implementations.  This is used by a topic to deliver
 * messages to the matching subscriptions.
 * 
 * 
 */
public interface DispatchPolicy {
    
    /**
     * Decides how to dispatch a selected message to a collection of consumers.  A safe
     * approach is to dispatch to every subscription that matches.
     *
     * The list will be safe to iterate over when this method is called
     * 
     * @return true if at least one consumer was dispatched or false if there are no active subscriptions that could be dispatched
     */
    boolean dispatch(MessageReference node, MessageEvaluationContext msgContext, List<Subscription> consumers) throws Exception;

}
