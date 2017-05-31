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

import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.component.jms.JmsMessage;
import org.apache.camel.impl.DefaultAsyncProducer;

import javax.jms.JMSException;
import java.util.Map;

public class BrokerProducer extends DefaultAsyncProducer {
    private final BrokerEndpoint brokerEndpoint;

    public BrokerProducer(BrokerEndpoint endpoint) {
        super(endpoint);
        brokerEndpoint = endpoint;
    }

    @Override
    public boolean process(Exchange exchange, AsyncCallback callback) {
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

    private ActiveMQMessage getMessage(Exchange exchange) throws IllegalStateException, JMSException {
        Message camelMessage = getMessageFromExchange(exchange);
        checkOriginalMessage(camelMessage);
        ActiveMQMessage result = (ActiveMQMessage) ((JmsMessage) camelMessage).getJmsMessage();
        applyNewHeaders(result, camelMessage.getHeaders());
        return result;
    }

    private Message getMessageFromExchange(Exchange exchange) {
        if (exchange.hasOut()) {
            return exchange.getOut();
        }

        return exchange.getIn();
    }

    private void checkOriginalMessage(Message camelMessage) throws IllegalStateException {
        /**
         * We purposely don't want to support injecting messages half-way through
         * broker processing - use the activemq camel component for that - but
         * we will support changing message headers and destinations.
         */

        if (!(camelMessage instanceof JmsMessage)) {
            throw new IllegalStateException("Not the original message from the broker " + camelMessage);
        }

        javax.jms.Message message = ((JmsMessage) camelMessage).getJmsMessage();

        if (!(message instanceof ActiveMQMessage)) {
            throw new IllegalStateException("Not the original message from the broker " + message);
        }
    }

    private void applyNewHeaders(ActiveMQMessage message, Map<String, Object> headers) throws JMSException {
        for (Map.Entry<String, Object> entry : headers.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if(value == null) {
                continue;
            }
            message.setObjectProperty(key, value.toString(), false);
        }
    }
}
