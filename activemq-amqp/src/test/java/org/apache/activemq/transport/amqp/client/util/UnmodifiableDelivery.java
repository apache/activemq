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
package org.apache.activemq.transport.amqp.client.util;

import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Record;
import org.apache.qpid.proton.engine.Sender;

/**
 * Unmodifiable Delivery wrapper used to prevent test code from accidentally
 * modifying Delivery state.
 */
public class UnmodifiableDelivery implements Delivery {

    private final Delivery delivery;

    public UnmodifiableDelivery(Delivery delivery) {
        this.delivery = delivery;
    }

    @Override
    public byte[] getTag() {
        return delivery.getTag();
    }

    @Override
    public Link getLink() {
        if (delivery.getLink() instanceof Sender) {
            return new UnmodifiableSender((Sender) delivery.getLink());
        } else if (delivery.getLink() instanceof Receiver) {
            return new UnmodifiableReceiver((Receiver) delivery.getLink());
        } else {
            throw new IllegalStateException("Delivery has unknown link type");
        }
    }

    @Override
    public DeliveryState getLocalState() {
        return delivery.getLocalState();
    }

    @Override
    public DeliveryState getRemoteState() {
        return delivery.getRemoteState();
    }

    @Override
    public int getMessageFormat() {
        return delivery.getMessageFormat();
    }

    @Override
    public void disposition(DeliveryState state) {
        throw new UnsupportedOperationException("Cannot alter the Delivery state");
    }

    @Override
    public void settle() {
        throw new UnsupportedOperationException("Cannot alter the Delivery state");
    }

    @Override
    public boolean isSettled() {
        return delivery.isSettled();
    }

    @Override
    public boolean remotelySettled() {
        return delivery.remotelySettled();
    }

    @Override
    public void free() {
        throw new UnsupportedOperationException("Cannot alter the Delivery state");
    }

    @Override
    public Delivery getWorkNext() {
        return new UnmodifiableDelivery(delivery.getWorkNext());
    }

    @Override
    public Delivery next() {
        return new UnmodifiableDelivery(delivery.next());
    }

    @Override
    public boolean isWritable() {
        return delivery.isWritable();
    }

    @Override
    public boolean isReadable() {
        return delivery.isReadable();
    }

    @Override
    public void setContext(Object o) {
        throw new UnsupportedOperationException("Cannot alter the Delivery state");
    }

    @Override
    public Object getContext() {
        return delivery.getContext();
    }

    @Override
    public boolean isUpdated() {
        return delivery.isUpdated();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Cannot alter the Delivery state");
    }

    @Override
    public boolean isPartial() {
        return delivery.isPartial();
    }

    @Override
    public int pending() {
        return delivery.pending();
    }

    @Override
    public boolean isBuffered() {
        return delivery.isBuffered();
    }

    @Override
    public Record attachments() {
        return delivery.attachments();
    }

    @Override
    public DeliveryState getDefaultDeliveryState() {
        return delivery.getDefaultDeliveryState();
    }

    @Override
    public void setDefaultDeliveryState(DeliveryState state) {
        throw new UnsupportedOperationException("Cannot alter the Delivery");
    }
}
