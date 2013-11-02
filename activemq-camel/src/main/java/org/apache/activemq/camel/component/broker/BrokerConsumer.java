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
import org.apache.activemq.broker.inteceptor.MessageInterceptor;
import org.apache.activemq.command.Message;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Processor;
import org.apache.camel.component.jms.JmsBinding;
import org.apache.camel.impl.DefaultConsumer;

public class BrokerConsumer extends DefaultConsumer implements MessageInterceptor {
    private final JmsBinding jmsBinding = new JmsBinding();

    public BrokerConsumer(Endpoint endpoint, Processor processor) {
        super(endpoint, processor);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        ((BrokerEndpoint) getEndpoint()).addMessageInterceptor(this);
    }

    @Override
    protected void doStop() throws Exception {
        ((BrokerEndpoint) getEndpoint()).removeMessageInterceptor(this);
        super.doStop();
    }

    @Override
    public void intercept(ProducerBrokerExchange producerExchange, Message message) {
        Exchange exchange = getEndpoint().createExchange(ExchangePattern.InOnly);

        exchange.setIn(new BrokerJmsMessage((javax.jms.Message) message, jmsBinding));
        exchange.setProperty(Exchange.BINDING, jmsBinding);
        exchange.setProperty(BrokerEndpoint.PRODUCER_BROKER_EXCHANGE, producerExchange);
        try {
            getProcessor().process(exchange);
        } catch (Exception e) {
            exchange.setException(e);
        }

        if (exchange.getException() != null) {
            getExceptionHandler().handleException("Error processing intercepted message: " + message, exchange, exchange.getException());
        }
    }

}
