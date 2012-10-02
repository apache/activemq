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

import java.util.EventObject;

import javax.jms.Destination;

import org.apache.activemq.command.ProducerId;

/**
 * An event when the number of producers on a given destination changes.
 * 
 * 
 */
public abstract class ProducerEvent extends EventObject {
    private static final long serialVersionUID = 2442156576867593780L;
    private final Destination destination;
    private final ProducerId producerId;
    private final int producerCount;

    public ProducerEvent(ProducerEventSource source, Destination destination, ProducerId producerId, int producerCount) {
        super(source);
        this.destination = destination;
        this.producerId = producerId;
        this.producerCount = producerCount;
    }

    public ProducerEventSource getAdvisor() {
        return (ProducerEventSource) getSource();
    }

    public Destination getDestination() {
        return destination;
    }

    /**
     * Returns the current number of producers active at the time this advisory was sent.
     * 
     */
    public int getProducerCount() {
        return producerCount;
    }

    public ProducerId getProducerId() {
        return producerId;
    }

    public abstract boolean isStarted();
}
