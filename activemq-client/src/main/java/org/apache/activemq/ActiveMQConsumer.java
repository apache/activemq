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
package org.apache.activemq;

import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.Session;

import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.util.JMSExceptionSupport;

public class ActiveMQConsumer implements JMSConsumer {
    
    private final ActiveMQContext activemqContext;
    private final MessageConsumer activemqMessageConsumer;

    ActiveMQConsumer(ActiveMQContext activemqContext, MessageConsumer activemqMessageConsumer) {
        this.activemqContext = activemqContext;
        this.activemqMessageConsumer = activemqMessageConsumer;
    }

    @Override
    public String getMessageSelector() {
        try {
            return activemqMessageConsumer.getMessageSelector();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public MessageListener getMessageListener() throws JMSRuntimeException {
        try {
            return activemqMessageConsumer.getMessageListener();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSRuntimeException {
        try {
            activemqMessageConsumer.setMessageListener(listener);
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public Message receive() {
        try {
            return activemqMessageConsumer.receive();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public Message receive(long timeout) {
        try {
            return activemqMessageConsumer.receive(timeout);
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public Message receiveNoWait() {
        try {
            return activemqMessageConsumer.receiveNoWait();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            activemqMessageConsumer.close();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }        
    }

    @Override
    public <T> T receiveBody(Class<T> c) {
        Message message = receive();
        if (message == null) {
            return null;
        }
        try {
            return message.getBody(c);
        } catch (JMSException e) {
            handleReceiveBodyFailure(message);
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public <T> T receiveBody(Class<T> c, long timeout) {
        Message message = receive(timeout);
        if (message == null) {
            return null;
        }
        try {
            return message.getBody(c);
        } catch (JMSException e) {
            handleReceiveBodyFailure(message);
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public <T> T receiveBodyNoWait(Class<T> c) {
        Message message = receiveNoWait();
        if (message == null) {
            return null;
        }
        try {
            return message.getBody(c);
        } catch (JMSException e) {
            handleReceiveBodyFailure(message);
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    /**
     * Handles failure of getBody() according to Jakarta Messaging 3.1 specification.
     * For AUTO_ACKNOWLEDGE or DUPS_OK_ACKNOWLEDGE modes, the message should be 
     * redelivered without incrementing the redelivery counter.
     * 
     * The JMS provider will behave as if the unsuccessful call to receiveBody had not occurred.
     * The message will be delivered again before any subsequent messages. This is not considered
     * to be redelivery and does not cause the JMSRedelivered message header field to be set or
     * the JMSXDeliveryCount message property to be incremented.
     */
    private void handleReceiveBodyFailure(Message message) {
        try {
            // Check if we're in AUTO_ACKNOWLEDGE or DUPS_OK_ACKNOWLEDGE mode
            int sessionMode = getSessionMode();
            if (sessionMode == Session.AUTO_ACKNOWLEDGE || sessionMode == Session.DUPS_OK_ACKNOWLEDGE) {
                // Save the current redelivery counter before rollback
                ActiveMQMessage activeMQMessage = null;
                int savedRedeliveryCounter = 0;
                if (message instanceof ActiveMQMessage) {
                    activeMQMessage = (ActiveMQMessage) message;
                    savedRedeliveryCounter = activeMQMessage.getRedeliveryCounter();
                }
                
                // Rollback to trigger redelivery without acknowledgment
                // According to spec, the message should be redelivered as if receiveBody had not occurred
                if (activemqMessageConsumer instanceof ActiveMQMessageConsumer) {
                    // Rollback will increment the counter, so we need to restore it immediately after
                    ((ActiveMQMessageConsumer) activemqMessageConsumer).rollback();
                    
                    // Restore the redelivery counter immediately to prevent it from being marked as redelivered
                    // This ensures the message is redelivered without incrementing JMSXDeliveryCount
                    if (activeMQMessage != null) {
                        // The counter may have been incremented by rollback(), restore it to original value
                        activeMQMessage.setRedeliveryCounter(savedRedeliveryCounter);
                    }
                }
            }
        } catch (JMSException e) {
            // If rollback fails, we can't do much, but we've already thrown the original exception
            // This exception will be swallowed since we're already throwing the getBody() exception
        }
    }

    /**
     * Gets the session acknowledgment mode.
     */
    private int getSessionMode() {
        try {
            activemqContext.checkContextState();
            if (activemqContext.activemqSession != null) {
                return activemqContext.activemqSession.getAcknowledgeMode();
            }
            // If session not created yet, use the sessionMode from context
            // We need to access it via reflection or make it accessible
            // For now, try to get it from the session
            return Session.AUTO_ACKNOWLEDGE; // Default fallback
        } catch (Exception e) {
            return Session.AUTO_ACKNOWLEDGE; // Default fallback
        }
    }

}
