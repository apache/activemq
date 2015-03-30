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

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.transport.amqp.client.util.UnmodifiableDelivery;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.message.Message;

public class AmqpMessage {

    private final AmqpReceiver receiver;
    private final Message message;
    private final Delivery delivery;

    private Map<Symbol, Object> messageAnnotationsMap;
    private Map<String, Object> applicationPropertiesMap;

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
    @SuppressWarnings("unchecked")
    public AmqpMessage(AmqpReceiver receiver, Message message, Delivery delivery) {
        this.receiver = receiver;
        this.message = message;
        this.delivery = delivery;

        if (message.getMessageAnnotations() != null) {
            messageAnnotationsMap = message.getMessageAnnotations().getValue();
        }

        if (message.getApplicationProperties() != null) {
            applicationPropertiesMap = message.getApplicationProperties().getValue();
        }
    }

    //----- Access to interal client resources -------------------------------//

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

    //----- Message disposition control --------------------------------------//

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

    //----- Convenience methods for constructing outbound messages -----------//

    /**
     * Sets the MessageId property on an outbound message using the provided String
     *
     * @param messageId
     *        the String message ID value to set.
     */
    public void setMessageId(String messageId) {
        checkReadOnly();
        lazyCreateProperties();
        getWrappedMessage().setMessageId(messageId);
    }

    /**
     * Return the set MessageId value in String form, if there are no properties
     * in the given message return null.
     *
     * @return the set message ID in String form or null if not set.
     */
    public String getMessageId() {
        if (message.getProperties() == null) {
            return null;
        }

        return message.getProperties().getMessageId().toString();
    }

    /**
     * Sets a given application property on an outbound message.
     *
     * @param key
     *        the name to assign the new property.
     * @param value
     *        the value to set for the named property.
     */
    public void setApplicationProperty(String key, Object value) {
        checkReadOnly();
        lazyCreateApplicationProperties();
        applicationPropertiesMap.put(key, value);
    }

    /**
     * Gets the application property that is mapped to the given name or null
     * if no property has been set with that name.
     *
     * @param key
     *        the name used to lookup the property in the application properties.
     *
     * @return the propety value or null if not set.
     */
    public Object getApplicationProperty(String key) {
        if (applicationPropertiesMap == null) {
            return null;
        }

        return applicationPropertiesMap.get(key);
    }

    /**
     * Perform a proper annotation set on the AMQP Message based on a Symbol key and
     * the target value to append to the current annotations.
     *
     * @param key
     *        The name of the Symbol whose value is being set.
     * @param value
     *        The new value to set in the annotations of this message.
     */
    public void setMessageAnnotation(String key, Object value) {
        checkReadOnly();
        lazyCreateMessageAnnotations();
        messageAnnotationsMap.put(Symbol.valueOf(key), value);
    }

    /**
     * Given a message annotation name, lookup and return the value associated with
     * that annotation name.  If the message annotations have not been created yet
     * then this method will always return null.
     *
     * @param key
     *        the Symbol name that should be looked up in the message annotations.
     *
     * @return the value of the annotation if it exists, or null if not set or not accessible.
     */
    public Object getMessageAnnotation(String key) {
        if (messageAnnotationsMap == null) {
            return null;
        }

        return messageAnnotationsMap.get(Symbol.valueOf(key));
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
        checkReadOnly();
        AmqpValue body = new AmqpValue(value);
        getWrappedMessage().setBody(body);
    }

    //----- Internal implementation ------------------------------------------//

    private void checkReadOnly() throws IllegalStateException {
        if (delivery != null) {
            throw new IllegalStateException("Message is read only.");
        }
    }

    private void lazyCreateMessageAnnotations() {
        if (messageAnnotationsMap == null) {
            messageAnnotationsMap = new HashMap<Symbol,Object>();
            message.setMessageAnnotations(new MessageAnnotations(messageAnnotationsMap));
        }
    }

    private void lazyCreateApplicationProperties() {
        if (applicationPropertiesMap == null) {
            applicationPropertiesMap = new HashMap<String, Object>();
            message.setApplicationProperties(new ApplicationProperties(applicationPropertiesMap));
        }
    }

    private void lazyCreateProperties() {
        if (message.getProperties() == null) {
            message.setProperties(new Properties());
        }
    }
}
