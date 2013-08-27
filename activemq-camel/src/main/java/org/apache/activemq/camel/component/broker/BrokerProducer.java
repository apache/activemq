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
package org.apache.activemq.camel.component.broker;

import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.camel.converter.ActiveMQMessageConverter;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.component.jms.JmsMessage;
import org.apache.camel.converter.ObjectConverter;
import org.apache.camel.impl.DefaultAsyncProducer;

public class BrokerProducer extends DefaultAsyncProducer {
    private final ActiveMQMessageConverter activeMQConverter = new ActiveMQMessageConverter();
    private final BrokerEndpoint brokerEndpoint;

    public BrokerProducer(BrokerEndpoint endpoint) {
        super(endpoint);
        brokerEndpoint = endpoint;
    }


    @Override
    public boolean process(Exchange exchange, AsyncCallback callback) {
        // deny processing if we are not started
        if (!isRunAllowed()) {
            if (exchange.getException() == null) {
                exchange.setException(new RejectedExecutionException());
            }
            // we cannot process so invoke callback
            callback.done(true);
            return true;
        }

        try {
            //In the middle of the broker - InOut doesn't make any sense
            //so we do in only
            return processInOnly(exchange, callback);

        } catch (Throwable e) {
            // must catch exception to ensure callback is invoked as expected
            // to let Camel error handling deal with this
            exchange.setException(e);
            callback.done(true);
            return true;
        }
    }

    protected boolean processInOnly(final Exchange exchange, final AsyncCallback callback) {
        try {
            ActiveMQMessage message = getMessage(exchange);
            if (message != null) {
                message.setDestination(brokerEndpoint.getDestination());
                //if the ProducerBrokerExchange is null the broker will create it
                ProducerBrokerExchange producerBrokerExchange = (ProducerBrokerExchange) exchange.getProperty(BrokerEndpoint.PRODUCER_BROKER_EXCHANGE);

                brokerEndpoint.inject(producerBrokerExchange, message);


            }
        } catch (Exception e) {
            exchange.setException(e);
        }
        callback.done(true);
        return true;
    }

    private ActiveMQMessage getMessage(Exchange exchange) throws Exception {
        ActiveMQMessage result = null;
        Message camelMesssage = null;
        if (exchange.hasOut()) {
            camelMesssage = exchange.getOut();
        } else {
            camelMesssage = exchange.getIn();
        }

        Map<String, Object> headers = camelMesssage.getHeaders();

        /**
         * We purposely don't want to support injecting messages half-way through
         * broker processing - use the activemq camel component for that - but
         * we will support changing message headers and destinations
         */
        if (camelMesssage instanceof JmsMessage) {
            JmsMessage jmsMessage = (JmsMessage) camelMesssage;
            if (jmsMessage.getJmsMessage() instanceof ActiveMQMessage) {
                result = (ActiveMQMessage) jmsMessage.getJmsMessage();
                //lets apply any new message headers
                setJmsHeaders(result, headers);
            } else {

                throw new IllegalStateException("not the original message from the broker " + jmsMessage.getJmsMessage());
            }
        } else {
            throw new IllegalStateException("not the original message from the broker " + camelMesssage);
        }
        return result;
    }

    private void setJmsHeaders(ActiveMQMessage message, Map<String, Object> headers) {
        message.setReadOnlyProperties(false);
        for (Map.Entry<String, Object> entry : headers.entrySet()) {
            if (entry.getKey().equalsIgnoreCase("JMSDeliveryMode")) {
                Object value = entry.getValue();
                if (value instanceof Number) {
                    Number number = (Number) value;
                    message.setJMSDeliveryMode(number.intValue());
                }
            }
            if (entry.getKey().equalsIgnoreCase("JmsPriority")) {
                Integer value = ObjectConverter.toInteger(entry.getValue());
                if (value != null) {
                    message.setJMSPriority(value.intValue());
                }
            }
            if (entry.getKey().equalsIgnoreCase("JMSTimestamp")) {
                Long value = ObjectConverter.toLong(entry.getValue());
                if (value != null) {
                    message.setJMSTimestamp(value.longValue());
                }
            }
            if (entry.getKey().equalsIgnoreCase("JMSExpiration")) {
                Long value = ObjectConverter.toLong(entry.getValue());
                if (value != null) {
                    message.setJMSExpiration(value.longValue());
                }
            }
            if (entry.getKey().equalsIgnoreCase("JMSRedelivered")) {
                message.setJMSRedelivered(ObjectConverter.toBool(entry.getValue()));
            }
            if (entry.getKey().equalsIgnoreCase("JMSType")) {
                Object value = entry.getValue();
                if (value != null) {
                    message.setJMSType(value.toString());
                }
            }
        }

    }
}
