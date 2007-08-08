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

import org.apache.activemq.command.ConsumerId;

import javax.jms.Destination;

import java.util.EventObject;

/**
 * An event when the number of consumers on a given destination changes.
 * 
 * @version $Revision$
 */
public abstract class ConsumerEvent extends EventObject {
    private static final long serialVersionUID = 2442156576867593780L;
    private final Destination destination;
    private final ConsumerId consumerId;
    private final int consumerCount;

    public ConsumerEvent(ConsumerEventSource source, Destination destination, ConsumerId consumerId, int consumerCount) {
        super(source);
        this.destination = destination;
        this.consumerId = consumerId;
        this.consumerCount = consumerCount;
    }

    public ConsumerEventSource getAdvisor() {
        return (ConsumerEventSource) getSource();
    }

    public Destination getDestination() {
        return destination;
    }

    /**
     * Returns the current number of consumers active at the time this advisory was sent.
     * 
     * Note that this is not the number of consumers active when the consumer started consuming.
     * It is usually more vital to know how many consumers there are now - rather than historically
     * how many there were when a consumer started. So if you create a {@link ConsumerListener}
     * after many consumers have started, you will receive a ConsumerEvent for each consumer. However the
     * {@link #getConsumerCount()} method will always return the current active consumer count on each event.
     */
    public int getConsumerCount() {
        return consumerCount;
    }

    public ConsumerId getConsumerId() {
        return consumerId;
    }

    public abstract boolean isStarted();
}
