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
package org.apache.activemq.advisory;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerId;

/**
 * An event generated when a consumer stops.
 * 
 * 
 */
public class ConsumerStoppedEvent extends ConsumerEvent {

    private static final long serialVersionUID = 5378835541037193206L;

    public ConsumerStoppedEvent(ConsumerEventSource source, ActiveMQDestination destination, ConsumerId consumerId, int count) {
        super(source, destination, consumerId, count);
    }

    public boolean isStarted() {
        return false;
    }

}
