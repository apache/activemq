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
package org.apache.activemq.broker;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ProducerInfo;

/**
 * This broker filter handles composite destinations. If a broker operation is
 * invoked using a composite destination, this filter repeats the operation
 * using each destination of the composite. HRC: I think this filter is
 * dangerous to use to with the consumer operations. Multiple Subscription
 * objects will be associated with a single JMS consumer each having a different
 * idea of what the current pre-fetch dispatch size is. If this is used, then
 * the client has to expect many more messages to be dispatched than the
 * pre-fetch setting allows.
 * 
 * @version $Revision: 1.8 $
 */
public class CompositeDestinationBroker extends BrokerFilter {

    public CompositeDestinationBroker(Broker next) {
        super(next);
    }

    /**
     * A producer may register to send to multiple destinations via a composite
     * destination.
     */
    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        // The destination may be null.
        ActiveMQDestination destination = info.getDestination();
        if (destination != null && destination.isComposite()) {
            ActiveMQDestination[] destinations = destination.getCompositeDestinations();
            for (int i = 0; i < destinations.length; i++) {
                ProducerInfo copy = info.copy();
                copy.setDestination(destinations[i]);
                next.addProducer(context, copy);
            }
        } else {
            next.addProducer(context, info);
        }
    }

    /**
     * A producer may de-register from sending to multiple destinations via a
     * composite destination.
     */
    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        // The destination may be null.
        ActiveMQDestination destination = info.getDestination();
        if (destination != null && destination.isComposite()) {
            ActiveMQDestination[] destinations = destination.getCompositeDestinations();
            for (int i = 0; i < destinations.length; i++) {
                ProducerInfo copy = info.copy();
                copy.setDestination(destinations[i]);
                next.removeProducer(context, copy);
            }
        } else {
            next.removeProducer(context, info);
        }
    }

    /**
     * 
     */
    public void send(ProducerBrokerExchange producerExchange, Message message) throws Exception {
        ActiveMQDestination destination = message.getDestination();
        if (destination.isComposite()) {
            ActiveMQDestination[] destinations = destination.getCompositeDestinations();
            for (int i = 0; i < destinations.length; i++) {
                if (i != 0) {
                    message = message.copy();
                }
                message.setOriginalDestination(destination);
                message.setDestination(destinations[i]);
                next.send(producerExchange, message);
            }
        } else {
            next.send(producerExchange, message);
        }
    }

}
