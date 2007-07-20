/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;

/**
 * A strategy for choosing which destination is used for dead letter queue messages.
 * 
 * @version $Revision$
 */
public interface DeadLetterStrategy {
    
    /**
     * Allow pluggable strategy for deciding if message should be sent to a dead letter queue
     * for example, you might not want to ignore expired or non-persistent messages
     * @param message
     * @return true if message should be sent to a dead letter queue
     */
    public boolean isSendToDeadLetterQueue(Message message);

    /**
     * Returns the dead letter queue for the given destination.
     */
    ActiveMQDestination getDeadLetterQueueFor(ActiveMQDestination originalDestination);

}
