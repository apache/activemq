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
package org.apache.activemq.transport.amqp.client;

import org.apache.activemq.transport.amqp.client.util.UnmodifiableDelivery;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;

public class AmqpMessage {

    private final AmqpReceiver receiver;
    private final Message message;
    private final Delivery delivery;

    /**
     * Creates a new AmqpMessage that wraps the information necessary to handle
     * an outgoing message.
     */
    public AmqpMessage() {
        receiver = null;
        delivery = null;

        message = Proton.message();
        message.setDurable(true);
    }

    /**
     * Creates a new AmqpMessage that wraps the information necessary to handle
     * an outgoing message.
     *
     * @param message
     *        the Proton message that is to be sent.
     */
    public AmqpMessage(Message message) {
        this(null, message, null);
    }

    /**
     * Creates a new AmqpMessage that wraps the information necessary to handle
     * an incoming delivery.
     *
     * @param receiver
     *        the AmqpReceiver that received this message.
     * @param message
     *        the Proton message that was received.
     * @param delivery
     *        the Delivery instance that produced this message.
     */
    public AmqpMessage(AmqpReceiver receiver, Message message, Delivery delivery) {
        this.receiver = receiver;
        this.message = message;
        this.delivery = delivery;
    }

    /**
     * Accepts the message marking it as consumed on the remote peer.
     *
     * @throws Exception if an error occurs during the accept.
     */
    public void accept() throws Exception {
        if (receiver == null) {
            throw new IllegalStateException("Can't accept non-received message.");
        }

        receiver.accept(delivery);
    }

    /**
     * Rejects the message, marking it as not deliverable here and failed to deliver.
     *
     * @throws Exception if an error occurs during the reject.
     */
    public void reject() throws Exception {
        reject(true, true);
    }

    /**
     * Rejects the message, marking it as failed to deliver and applying the given value
     * to the undeliverable here tag.
     *
     * @param undeliverableHere
     *        marks the delivery as not being able to be process by link it was sent to.
     *
     * @throws Exception if an error occurs during the reject.
     */
    public void reject(boolean undeliverableHere) throws Exception {
        reject(undeliverableHere, true);
    }

    /**
     * Rejects the message, marking it as not deliverable here and failed to deliver.
     *
     * @param undeliverableHere
     *        marks the delivery as not being able to be process by link it was sent to.
     * @param deliveryFailed
     *        indicates that the delivery failed for some reason.
     *
     * @throws Exception if an error occurs during the reject.
     */
    public void reject(boolean undeliverableHere, boolean deliveryFailed) throws Exception {
        if (receiver == null) {
            throw new IllegalStateException("Can't reject non-received message.");
        }

        receiver.reject(delivery, undeliverableHere, deliveryFailed);
    }

    /**
     * Release the message, remote can redeliver it elsewhere.
     *
     * @throws Exception if an error occurs during the reject.
     */
    public void release() throws Exception {
        if (receiver == null) {
            throw new IllegalStateException("Can't release non-received message.");
        }

        receiver.release(delivery);
    }

    /**
     * @return the AMQP Delivery object linked to a received message.
     */
    public Delivery getWrappedDelivery() {
        if (delivery != null) {
            return new UnmodifiableDelivery(delivery);
        }

        return null;
    }

    /**
     * @return the AMQP Message that is wrapped by this object.
     */
    public Message getWrappedMessage() {
        return message;
    }

    /**
     * @return the AmqpReceiver that consumed this message.
     */
    public AmqpReceiver getAmqpReceiver() {
        return receiver;
    }

    /**
     * Sets a String value into the body of an outgoing Message, throws
     * an exception if this is an incoming message instance.
     *
     * @param value
     *        the String value to store in the Message body.
     *
     * @throws IllegalStateException if the message is read only.
     */
    public void setText(String value) throws IllegalStateException {
        if (delivery != null) {
            throw new IllegalStateException("Message is read only.");
        }

        AmqpValue body = new AmqpValue(value);
        getWrappedMessage().setBody(body);
    }
}
