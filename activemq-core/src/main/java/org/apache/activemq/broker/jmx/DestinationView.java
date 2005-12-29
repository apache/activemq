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
package org.apache.activemq.broker.jmx;

import org.apache.activemq.broker.region.Destination;

public class DestinationView implements DestinationViewMBean {

    private final Destination destination;

    public DestinationView(Destination destination) {
        this.destination = destination;
    }

    public void gc() {
        destination.gc();
    }
    public void resetStatistics() {
        destination.getDestinationStatistics().reset();
    }

    public long getEnqueueCount() {
        return destination.getDestinationStatistics().getEnqueues().getCount();
    
    }
    public long getDequeueCount() {
        return destination.getDestinationStatistics().getDequeues().getCount();
    }

    public long getConsumerCount() {
        return destination.getDestinationStatistics().getConsumers().getCount();
    }
    
    public long getMessages() {
        return destination.getDestinationStatistics().getMessages().getCount();
    }
    
    public long getMessagesCached() {
        return destination.getDestinationStatistics().getMessagesCached().getCount();
    }
    
}
