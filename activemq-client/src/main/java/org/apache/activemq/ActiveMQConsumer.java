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

import javax.jms.JMSConsumer;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

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
        throw new UnsupportedOperationException("receiveBody(Class<T>) is not supported");
    }

    @Override
    public <T> T receiveBody(Class<T> c, long timeout) {
        throw new UnsupportedOperationException("receiveBody(Class<T>, long) is not supported");
    }

    @Override
    public <T> T receiveBodyNoWait(Class<T> c) {
        throw new UnsupportedOperationException("receiveBodyNoWait(Class<T>) is not supported");
    }

}
